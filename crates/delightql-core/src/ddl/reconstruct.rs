// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Stored definition source, read back into a typed group.
//!
//! The catalog stores a definition's SOURCE, and it has to: normalization
//! interns every name into the compilation's `names::Registry`, so a group
//! built while consulting carries that compilation's arena and must never be
//! handed to a later one. The boundary is therefore real and PER-COMPILATION —
//! one road from stored clause text to the typed group a resolver expects, and
//! not thirty-three of them.
//!
//! What this is NOT is a second parser. Every entry here goes through the one
//! consolidated grammar and the one normalization; the only thing that makes
//! it a reconstruction rather than a parse is where the bytes came from.

use crate::diagnostic::Internal;
use crate::error::Result;

use crate::pipeline::asts::core::Query;
use crate::pipeline::asts::ddl::{Clause, ClauseDecl};
use crate::pipeline::normalize::Normalized;
use std::rc::Rc;

/// One subject's clauses, read from the source the catalog stored.
///
/// The source holds the clauses of ONE subject in authored order — that is
/// what `entity_clause` keeps and what a group's reconstruction asks for.
pub fn clauses(source: &str) -> Result<Vec<ClauseDecl>> {
    normalized(source).map(Normalized::into_definitions)
}

/// The same, with the danger and option annotations the source states
/// beside its clauses (a definition's own declarations).
pub fn clauses_declared(source: &str) -> Result<(Vec<ClauseDecl>, crate::pipeline::normalize::Sidecars)> {
    normalized(source).map(|mut normalized| {
        let declared = std::mem::take(&mut normalized.declared);
        (normalized.into_definitions(), declared)
    })
}

/// The same, assembled. Every clause law runs in `DefinitionGroup::assemble`,
/// before a caller can register a name or mint a scope. A stored source is
/// ONE subject's clauses — what `entity_clause` keeps.
#[cfg(test)]
pub fn group(source: &str) -> Result<crate::pipeline::asts::ddl::DefinitionGroup> {
    let Some(family) =
        crate::pipeline::asts::ddl::ClauseFamily::gather_one(clauses(source)?)?
    else {
        return Err(Internal::invariant(
            "ddl::reconstruct",
            format!(
                "No definition found in source: '{}'",
                crate::pipeline::parse::truncate_for_display(source, 60)
            ),
        ));
    };
    crate::pipeline::asts::ddl::DefinitionGroup::assemble(family)
}

/// ONE PARAMETERIZED CLAUSE'S BODY, read again from its text before any
/// use — the reference census's reading.
pub fn analysis_clause_body(clause: &Clause) -> Result<Query> {
    reread(clause)
}

fn reread(clause: &Clause) -> Result<Query> {
    let text = clause.body_text.as_ref().ok_or_else(|| {
        Internal::invariant(
            "ddl::reconstruct",
            "a parameterized clause is read again only from the text its reading recorded",
        )
    })?;
    let tree = crate::pipeline::parse::query_sequence(&text.source)?;
    let registry = Rc::new(crate::names::Registry::new(&[]));
    crate::pipeline::normalize::reread_body(
        &tree,
        registry,
        text.enclosing.clone(),
        clause.params(),
    )
}

fn normalized(source: &str) -> Result<Normalized> {
    let tree = crate::pipeline::parse::definition_file(source)?;
    let registry = Rc::new(crate::names::Registry::new(&[]));
    crate::pipeline::normalize::stored_definition_file(&tree, registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::reconstruct;
    use crate::pipeline::asts::core::definitions::HoParam;
    use crate::pipeline::asts::core::{DomainExpression, FunctionApplication};
    use crate::pipeline::asts::ddl::DdlBody;
    use crate::pipeline::asts::ddl::DefKind;

    /// A scalar parameter's name and whether it carries a guard.
    fn scalar_param(param: &HoParam) -> (String, bool) {
        match param {
            HoParam::Scalar { name, guard, .. } => (name.to_string(), guard.is_some()),
            other => panic!("expected a scalar parameter, got {other:?}"),
        }
    }

    /// SUPPLY IS ELABORATION, on an HO view's OUTPUT positions too: a ground
    /// head term supplies the constant and its `as` label NAMES the position,
    /// so the assembled heading answers to the label — the builder must have
    /// somewhere to put it rather than refusing.
    #[test]
    fn a_head_as_label_names_an_ho_output_position() {
        let source = r#"labeled(T(*))("vip" as tag, last_name) :- T(*), age > 40"#;
        let group = reconstruct::group(source).expect("a labelled ground head term builds");
        let items = group.first().head.items.listed().expect("a listed head");
        assert_eq!(
            items[0].offered_name().map(|name| name.to_string()),
            Some("tag".to_string())
        );
        assert_eq!(items[0].supply.spelling(), "\"vip\"");

        // Control: the same head WITHOUT a label builds too.
        let ok = r#"labeled(T(*))(tag, last_name) :- T(*), age > 40"#;
        assert!(reconstruct::group(ok).is_ok());
    }

    /// Pins the non-ASCII slicing panic's sibling: the "No definition found"
    /// message truncated the source at byte 60 without a char-boundary
    /// check, panicking on multi-byte content.
    #[test]
    fn no_definition_error_truncation_is_char_boundary_safe() {
        // A `?-` query statement (skipped by build_ddl_file) with multi-byte
        // content: 19 ASCII bytes then 20 3-byte chars; byte 60 is mid-char
        // (60 - 19 = 41, 41 % 3 == 2).
        let source = format!("?- users(*), nm = \"{}\"", "─".repeat(20));
        let err = reconstruct::group(&source).expect_err("no definition in source");
        let msg = err.to_string();
        assert!(
            msg.contains("No definition found"),
            "expected the normal no-definition error, got: {msg}"
        );
    }

    #[test]
    fn test_build_function_definition() {
        let source = "double:(x) :- x * 2";
        let defs = reconstruct::group(source).unwrap().into_clauses();
        assert_eq!(defs.len(), 1);

        let def = &defs[0];
        let params = def.params();
        assert_eq!(params.len(), 1);
        assert_eq!(scalar_param(&params[0]), ("x".to_string(), false));

        // Body should be a scalar (DomainExpression)
        let expr = def.as_scalar_body().expect("expected scalar body");
        match expr {
            DomainExpression::Application(FunctionApplication::Infix(infix)) => {
                assert_eq!(
                    infix.operator,
                    crate::pipeline::asts::vocabulary::BinOp::Mul
                );
            }
            other => panic!("Expected infix multiply, got: {:?}", other),
        }
    }

    #[test]
    fn test_build_view_definition() {
        let source = "active_users(*) :- users(*), balance > 1000";
        let defs = reconstruct::group(source).unwrap().into_clauses();
        assert_eq!(defs.len(), 1);

        let def = &defs[0];
        assert!(def.head.is_glob());

        // Body should be relational
        assert!(matches!(def.body, DdlBody::Relational(_)));
    }

    /// Two subjects are two definitions, and a group is ONE subject: the
    /// door refuses rather than registering both under the first one's name.
    #[test]
    fn two_subjects_are_not_one_group() {
        let source = "double:(x) :- x * 2\ntriple:(x) :- x * 3";
        let err = reconstruct::group(source).unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/group/mixed_subject"
        );
    }

    #[test]
    fn test_full_source_preserved() {
        let source = "double:(x) :- x * 2";
        let defs = reconstruct::group(source).unwrap().into_clauses();
        assert_eq!(defs[0].full_source, "double:(x) :- x * 2");
    }

    #[test]
    fn test_into_domain_expr() {
        let source = "double:(x) :- x * 2";
        let defs = reconstruct::group(source).unwrap().into_clauses();
        let def = defs.into_iter().next().unwrap();
        let expr = def.into_scalar_body().expect("expected scalar body");
        match &expr {
            DomainExpression::Application(FunctionApplication::Infix(infix)) => {
                assert_eq!(
                    infix.operator,
                    crate::pipeline::asts::vocabulary::BinOp::Mul
                );
            }
            other => panic!("Expected infix multiply, got: {:?}", other),
        }
    }

    #[test]
    fn test_build_single_definition_function() {
        let group = reconstruct::group("double:(x) :- x * 2").unwrap();
        assert_eq!(group.name(), "double");
        assert_eq!(group.kind(), DefKind::Function);
        assert!(matches!(group.first().body, DdlBody::Scalar(_)));
    }

    #[test]
    fn test_build_single_definition_view() {
        let group = reconstruct::group("active_users(*) :- users(*)").unwrap();
        assert_eq!(group.name(), "active_users");
        assert_eq!(group.kind(), DefKind::View);
        assert!(matches!(group.first().body, DdlBody::Relational(_)));
    }

    #[test]
    fn test_build_single_definition_empty_fails() {
        assert!(reconstruct::group("").is_err());
    }

    #[test]
    fn test_build_ddl_file_multi_clause_same_name() {
        // A FUNCTION RULE'S BODY IS A DOMEX (FN.30), so a value function's
        // clauses carry value bodies and select by guard.
        let source = "sign:(x | x < 0) :- 0 - x\nsign:(x) :- x";
        let group = reconstruct::group(source).unwrap();
        assert_eq!(group.name(), "sign");
        assert_eq!(group.clauses().len(), 2);
        // Both should be scalar bodies
        assert!(matches!(group.clauses()[0].body, DdlBody::Scalar(_)));
        assert!(matches!(group.clauses()[1].body, DdlBody::Scalar(_)));
    }

    /// Interleaved subjects are still more than one subject.
    #[test]
    fn interleaved_subjects_are_not_one_group() {
        let source = "double:(x) :- x * 2\ntriple:(x) :- x * 3\ndouble:(x) :- x + x";
        let err = reconstruct::group(source).unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/group/mixed_subject"
        );
    }

    /// A deferred BODY is not an absent GROUP.
    ///
    /// A higher-order template whose bound names a scalar formal is read
    /// where it is declared: the bound records the formal it names, and the
    /// group assembles: a subject, a kind, an arity, and a head are all
    /// written LEFT of the neck.
    #[test]
    fn a_bound_naming_a_formal_reads_and_assembles_its_group() {
        const TEMPLATE: &str = "T(*), #<$.n";

        let one = reconstruct::group(&format!("pick(T(*), n)(a, b) :- {TEMPLATE}"))
            .expect("a body whose bound names a formal builds a group");
        let DdlBody::Relational(query) = &one.first().body else {
            panic!("the body is read, not deferred");
        };
        let bounds: Vec<_> = query
            .body
            .steps()
            .iter()
            .flat_map(|step| step.form().bound_formals())
            .collect();
        assert_eq!(bounds.len(), 1, "the bound records the formal it names");
        assert_eq!(one.name(), "pick");
        assert_eq!(one.kind(), DefKind::HoView);
        assert_eq!(
            one.first().head.bound_param_names().len(),
            2,
            "the fronts are complete"
        );

        // And the laws ran over it: two clauses offering different names at
        // position 1 refuse, with nothing but their heads to decide on.
        let err = reconstruct::group(&format!(
            "pick(T(*), n)(a, b) :- {TEMPLATE}\npick(T(*), n)(b, a) :- {TEMPLATE}"
        ))
        .expect_err("disagreeing heads refuse even with deferred bodies");
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/head/name_conflict"
        );
    }

    /// The clause laws run in the ONE door, before any caller can register
    /// a name: mixed kinds and the head algebra refuse at build.
    #[test]
    fn the_group_door_runs_the_clause_laws() {
        let mixed_kind = reconstruct::group("foo:(x) :- x + 1\nfoo(x) :- x > 0")
            .expect_err("a function and a sigma are not one definition");
        assert_eq!(
            mixed_kind.error_uri(),
            "delightql-error://semantic/ddl/head/mixed_kind"
        );

        let head_forms =
            reconstruct::group("data(*) :- users(*)\ndata(first_name, age) :- users(*)")
                .expect_err("a glob head and a listed head are not one contract");
        assert_eq!(
            head_forms.error_uri(),
            "delightql-error://semantic/ddl/head/mixed_forms"
        );
    }

    #[test]
    fn test_build_function_with_guard() {
        let source = "fizzbuzz:(n | (n % 15) = 0) :- \"fizzbuzz\"";
        let defs = reconstruct::group(source).unwrap().into_clauses();
        assert_eq!(defs.len(), 1);
        let params = defs[0].params();
        assert_eq!(params.len(), 1);
        assert_eq!(scalar_param(&params[0]), ("n".to_string(), true));
    }

    #[test]
    fn test_build_function_without_guard_still_works() {
        // A parameter needs no guard: the unguarded spelling is the plain one.
        let source = "double:(x) :- x * 2";
        let group = reconstruct::group(source).unwrap();
        assert_eq!(group.clauses().len(), 1);

        assert_eq!(group.kind(), DefKind::Function);
        let params = group.first().params();
        assert_eq!(params.len(), 1);
        assert_eq!(scalar_param(&params[0]), ("x".to_string(), false));
    }

    #[test]
    fn test_build_multi_clause_with_guards() {
        let source = concat!(
            "fizzbuzz:(n | (n % 15) = 0) :- \"fizzbuzz\"\n",
            "fizzbuzz:(n | (n % 3) = 0) :- \"fizz\"\n",
            "fizzbuzz:(n | (n % 5) = 0) :- \"buzz\"\n",
            "fizzbuzz:(n) :- n"
        );
        let defs = reconstruct::group(source).unwrap().into_clauses();
        assert_eq!(defs.len(), 4);

        // First three have guards
        for (i, def) in defs.iter().take(3).enumerate() {
            assert!(
                scalar_param(&def.params()[0]).1,
                "Clause {i} should have a guard"
            );
        }

        // Last one has no guard (default case)
        assert!(
            !scalar_param(&defs[3].params()[0]).1,
            "Default clause should have no guard"
        );
    }

    #[test]
    fn test_build_sigma_predicate() {
        let source = "empty(column) :- null = column";
        let group = reconstruct::group(source).unwrap();
        assert_eq!(group.clauses().len(), 1);
        assert_eq!(group.name(), "empty");
        assert_eq!(group.kind(), DefKind::Sigma);

        let def = group.first();
        {
            let params = def.params();
            assert_eq!(params.len(), 1);
            assert_eq!(scalar_param(&params[0]), ("column".to_string(), false));
        }

        // A sigma rule's body is a TRUTH, and the carrier says so.
        assert!(def.as_truth_expr().is_some());
        assert!(def.as_scalar_body().is_none());
    }

    #[test]
    fn test_build_multi_clause_sigma_predicate() {
        let source = concat!(
            "empty(column) :- null = column\n",
            "empty(column) :- trim:(column) = \"\""
        );
        let group = reconstruct::group(source).unwrap();
        assert_eq!(group.name(), "empty");
        assert_eq!(group.kind(), DefKind::Sigma);
        let defs = group.into_clauses();
        assert_eq!(defs.len(), 2);

        // Both clauses carry a truth body.
        assert!(defs[0].as_truth_expr().is_some());
        assert!(defs[1].as_truth_expr().is_some());
    }

    #[test]
    fn test_sigma_predicate_entity_type() {
        let group = reconstruct::group("empty(column) :- null = column").unwrap();
        assert_eq!(
            group.entity_type(),
            crate::enums::EntityType::DqlTemporarySigmaRule
        );
    }

    /// `foo:(x)` is a function and `foo(x)` is a sigma predicate — two kinds
    /// of entity under one spelling, which is not one definition.
    #[test]
    fn test_mixed_function_and_sigma_types() {
        let source = "foo:(x) :- x + 1\nfoo(x) :- x > 0";
        let err = reconstruct::group(source).unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/head/mixed_kind"
        );
        assert_eq!(
            reconstruct::group("foo:(x) :- x + 1")
                .unwrap()
                .entity_type(),
            crate::enums::EntityType::DqlFunctionExpression,
            "foo:(x) should be Function"
        );
        assert_eq!(
            reconstruct::group("foo(x) :- x > 0").unwrap().entity_type(),
            crate::enums::EntityType::DqlTemporarySigmaRule,
            "foo(x) should be SigmaPredicate"
        );
    }

    #[test]
    fn test_build_fact_definition() {
        let source = r#"person(0, "Gusti", "Parlor")"#;
        let defs = reconstruct::group(source).unwrap().into_clauses();
        assert_eq!(defs.len(), 1);

        let def = &defs[0];
        // Body should be relational (anonymous table)
        assert!(matches!(def.body, DdlBody::Relational(_)));
    }

    #[test]
    fn fact_function_entity_type_owns_relational_capability() {
        use crate::enums::EntityType;

        let total =
            reconstruct::group(r#"grade(score -> letter ---- 90 -> "A"; _ -> "F")"#).unwrap();
        assert_eq!(
            total.entity_type(),
            EntityType::DqlDefaultFactFunctionExpression
        );
        assert!(!total.entity_type().realizes_relation());

        let finite =
            reconstruct::group(r#"grade(score -> letter ---- 90 -> "A"; null -> "unknown")"#)
                .unwrap();
        assert_eq!(finite.entity_type(), EntityType::DqlFactExpression);
        assert!(finite.entity_type().realizes_relation());
    }

    #[test]
    fn test_build_stacked_fact_definition() {
        let source = r#"employee(Id, Name --- 0, "Gusti"; 1, "Diane")"#;
        let group = reconstruct::group(source).unwrap();
        assert_eq!(group.clauses().len(), 1);
        assert_eq!(group.name(), "employee");
        assert_eq!(group.kind(), DefKind::Fact);
        assert!(matches!(group.first().body, DdlBody::Relational(_)));
    }

    #[test]
    fn test_build_multiple_same_name_facts() {
        let source = "person(0, \"Gusti\")\nperson(1, \"Diane\")";
        let group = reconstruct::group(source).unwrap();
        assert_eq!(group.name(), "person");
        assert_eq!(group.kind(), DefKind::Fact);
        assert_eq!(group.clauses().len(), 2);
    }

    /// A fact and a function are two subjects here, and would be two kinds
    /// even under one spelling: neither is one definition.
    #[test]
    fn test_mixed_facts_and_functions() {
        let source = "person(0, \"Gusti\")\ndouble:(x) :- x * 2";
        let err = reconstruct::group(source).unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/group/mixed_subject"
        );
    }

    #[test]
    fn test_build_view_with_docs() {
        let source =
            "high_balance(*) :- (~~docs Users with balance over 1000. ~~) users(*), balance > 1000";
        let group = reconstruct::group(source).unwrap();
        assert_eq!(group.name(), "high_balance");
        let defs = group.into_clauses();
        assert_eq!(defs.len(), 1);
        assert_eq!(
            defs[0].doc.as_deref(),
            Some("Users with balance over 1000.")
        );
    }

    #[test]
    fn test_build_function_with_docs() {
        let source = "double:(x) :- (~~docs Multiplies by two. ~~) x * 2";
        let group = reconstruct::group(source).unwrap();
        assert_eq!(group.name(), "double");
        assert_eq!(group.doc(), Some("Multiplies by two."));
    }

    #[test]
    fn test_build_no_docs_is_none() {
        let source = "double:(x) :- x * 2";
        let defs = reconstruct::group(source).unwrap().into_clauses();
        assert!(defs[0].doc.is_none());
    }

    /// One subject's clauses, read back from the source the catalog stored.
    #[test]
    fn stored_clause_source_reassembles_its_group() {
        let stored = "person(0, \"Gusti\")\nperson(1, \"Diane\")";
        let rebuilt = group(stored).unwrap();
        assert_eq!(rebuilt.name(), "person");
        assert_eq!(rebuilt.kind(), DefKind::Fact);
        assert_eq!(rebuilt.clauses().len(), 2);
    }
}

#[cfg(test)]
mod probe {
    #[test]
    fn probe_effect_body() {
        let src = "main!(*) :-\n    source.orders(*), amount > 0 |> temp_table!(staged(*))(*) : s!\n    s!(*) |> returning!(*)\n";
        let g = super::group(src).expect("group");
        for c in g.clauses() {
            if let crate::pipeline::asts::ddl::DdlBody::Relational(q) = &c.body {
                println!("BODY = {}", {
                    use crate::lispy::ToLispy;
                    q.to_lispy()
                });
            }
        }
    }
}

/// A compilation's depth budget reaches the definitions it reads back.
///
/// Reconstruction is not a tooling entrance: `group`, `bound_clause_body` and their
/// siblings are called during resolution, grounding, effect transformation and
/// consulted-view expansion, inside a compilation that has already armed. If
/// these parses asked process policy again, a host moving that policy could
/// let a stored body pass the boundary its caller armed — or refuse one the
/// caller could afford — while `compiler_limit(*)` reported the caller's
/// number either way.
///
/// Every pin here reads the budget off a REFUSAL, and the ladder is deeper
/// than either budget under test, so no assertion depends on walking a tree
/// that deep. A test thread's stack is a fraction of the main one's, and the
/// ceiling bounds configuration rather than physics: a walk near it aborts the
/// process instead of failing a test.
#[cfg(test)]
mod armed_depth_tests {
    use crate::compiler_limits::{ArmedLimits, ProcessLimitLease, Running, NESTING};

    /// Deeper than both budgets below, so BOTH refuse it and the number the
    /// refusal states is what discriminates.
    const DEEP: usize = 1090;
    const LOWER: usize = 700;
    const HIGHER: usize = 1000;

    fn clause(levels: usize) -> String {
        format!(
            "deep(v) :- users(*) |> ({}age{} as v)",
            "(".repeat(levels),
            ")".repeat(levels)
        )
    }

    fn body(levels: usize) -> String {
        format!(
            "users(*) |> ({}age{} as v)",
            "(".repeat(levels),
            ")".repeat(levels)
        )
    }

    /// Measured without the guard, so the premise holds for a tree no budget
    /// under test affords. Tree-sitter builds iteratively; this costs no
    /// stack.
    fn depth_of(source: &str) -> usize {
        crate::pipeline::syntax::Parser::new()
            .parse_definition_file(source)
            .depth()
    }

    fn refused_budget(error: &crate::error::DelightQLError) -> String {
        assert!(
            error.error_uri().contains("operational/resource/nesting"),
            "expected the depth refusal, got {}",
            error.error_uri()
        );
        error.to_string()
    }

    #[test]
    fn a_stored_body_is_judged_by_the_running_compilation_not_later_policy() {
        let _lease = ProcessLimitLease::take();
        let source = clause(DEEP);
        let depth = depth_of(&source);
        assert!(
            depth > HIGHER,
            "the ladder must be past both budgets so only the stated one \
             discriminates, got {depth}"
        );

        // Arm low, then raise policy. A road that re-read policy would answer
        // with 1000; the running compilation must answer with what it armed.
        NESTING.set(LOWER);
        let _running = Running::under(std::rc::Rc::new(ArmedLimits::from_policy()));
        NESTING.set(HIGHER);

        let refused = refused_budget(&super::clauses(&source).expect_err("past every budget"));
        assert!(
            refused.contains(&LOWER.to_string()),
            "the refusal must state the depth this compilation armed: {refused}"
        );
        assert!(
            !refused.contains(&HIGHER.to_string()),
            "and must not state the policy it never armed: {refused}"
        );
    }

    /// The same claim the other way round, so the pin above is not just
    /// reading whichever number happens to be smaller.
    #[test]
    fn arming_high_and_lowering_policy_still_answers_with_the_armed_value() {
        let _lease = ProcessLimitLease::take();
        let source = clause(DEEP);

        NESTING.set(HIGHER);
        let _running = Running::under(std::rc::Rc::new(ArmedLimits::from_policy()));
        NESTING.set(LOWER);

        let refused = refused_budget(&super::clauses(&source).expect_err("past every budget"));
        assert!(
            refused.contains(&HIGHER.to_string()),
            "the refusal must state the depth this compilation armed: {refused}"
        );
    }

    /// The guard is not simply refusing everything: an ordinary stored clause
    /// reconstructs under the same arrangement.
    #[test]
    fn an_ordinary_stored_clause_still_reconstructs() {
        let _lease = ProcessLimitLease::take();
        NESTING.set(LOWER);
        let _running = Running::under(std::rc::Rc::new(ArmedLimits::from_policy()));
        NESTING.set(HIGHER);

        let group = super::group("person(0, \"Gusti\")").expect("a shallow clause is afforded");
        assert_eq!(group.name(), "person");
    }

    /// Every reconstruction entrance takes the same road, so none of them can
    /// be the one that still asks policy.
    #[test]
    fn every_reconstruction_entrance_answers_to_the_armed_budget() {
        let _lease = ProcessLimitLease::take();
        let source = clause(DEEP);
        let bare = body(DEEP);

        NESTING.set(LOWER);
        let _running = Running::under(std::rc::Rc::new(ArmedLimits::from_policy()));
        NESTING.set(HIGHER);

        let held = crate::pipeline::asts::ddl::Clause {
            head: crate::pipeline::asts::core::definitions::Head::glob(),
            body: crate::pipeline::asts::ddl::DdlBody::Deferred,
            full_source: source.clone(),
            doc: None,
            body_text: Some(crate::pipeline::asts::ddl::BodyText {
                source: bare,
                enclosing: Default::default(),
            }),
        };
        for (entrance, error) in [
            ("clauses", super::clauses(&source).expect_err("clauses")),
            ("group", super::group(&source).expect_err("group")),
            (
                "analysis_clause_body",
                super::analysis_clause_body(&held).expect_err("analysis_clause_body"),
            ),
        ] {
            let refused = refused_budget(&error);
            assert!(
                refused.contains(&LOWER.to_string()),
                "{entrance} answered to policy rather than to what was armed: {refused}"
            );
        }
    }
}

/// A family holding facts reaches registration as authored: the assembler
/// decides its kind and leaves its clause agreement to the compile road's
/// family judgment.
#[cfg(test)]
mod fact_family_pins {
    use super::*;
    use crate::pipeline::asts::ddl::DefKind;

    /// Mixed fact and relational clauses are the one ruled kind union: the
    /// group is a relational definition. Any other mix is two kinds under
    /// one spelling.
    #[test]
    fn mixed_fact_and_rule_clauses_are_one_relational_definition() {
        let built = group("b(\"seed\", \"X\")\nb(tag, x) :- _(tag, x ---- \"r\", \"Y\")").unwrap();
        assert_eq!(built.kind(), DefKind::View);
        for other in ["b(1)\nb:(x) :- x + 1", "b(1)\nb(x) :- x > 0"] {
            let err = group(other).unwrap_err();
            assert_eq!(
                err.error_uri(),
                "delightql-error://semantic/ddl/head/mixed_kind"
            );
        }
    }

    /// The Ground-Position rule still binds a rule's head: a ground head
    /// position nobody names refuses.
    #[test]
    fn a_rule_head_ground_position_must_be_named() {
        let rule = group(r#"b("c" as c, "d") :- _(1)"#).unwrap_err();
        assert_eq!(
            rule.error_uri(),
            "delightql-error://semantic/ddl/head/unnamed_ground_position"
        );
    }

    /// A headerless parameterized fact's datum label has no verbose-form
    /// equivalent, so it refuses toward the header spelling instead of
    /// silently disappearing.
    #[test]
    fn a_headerless_parameterized_fact_offer_refuses() {
        let err = clauses("hof(T(*))(1 as a)").unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/head/parameterized_fact_offer"
        );
    }
}
