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

use crate::error::{DelightQLError, Result};

use crate::pipeline::asts::core::Query;
use crate::pipeline::asts::ddl::{ClauseDecl, DdlBody, DefinitionGroup};
use crate::pipeline::normalize::Normalized;
use crate::pipeline::query_features::HoParamBindings;
use crate::pipeline::syntax::{cst, SyntaxTree, TypedNode};
use std::rc::Rc;

/// One subject's clauses, read from the source the catalog stored.
///
/// The source holds the clauses of ONE subject in authored order — that is
/// what `entity_clause` keeps and what a group's reconstruction asks for.
pub fn clauses(source: &str) -> Result<Vec<ClauseDecl>> {
    normalized(source, None).map(|normalized| normalized.definitions)
}

/// The same, assembled. Every clause law runs in `DefinitionGroup::assemble`,
/// before a caller can register a name or mint a scope.
///
/// MEMOIZED FOR THE LIFE OF ONE COMPILATION. A subject asked for five times in
/// one compilation is read once; the memo is opened and closed by
/// [`Compilation`], so nothing built under one compilation's arena can be
/// handed to the next.
pub fn group(source: &str) -> Result<DefinitionGroup> {
    if let Some(hit) = Compilation::cached(source) {
        return Ok(hit);
    }
    let built = assemble(source)?;
    Compilation::remember(source, &built);
    Ok(built)
}

fn assemble(source: &str) -> Result<DefinitionGroup> {
    let decls = clauses(source)?;
    if decls.is_empty() {
        return Err(DelightQLError::parse_error(format!(
            "No definition found in source: '{}'",
            crate::pipeline::parse::truncate_for_display(source, 60)
        )));
    }
    DefinitionGroup::assemble(decls)
}

thread_local! {
    /// Open memos, innermost last. A compilation may open another inside
    /// itself — a consulted definition's instantiation is one — and each
    /// keeps its own, so nothing
    /// a nested compilation read outlives it.
    static MEMOS: std::cell::RefCell<Vec<(u64, std::collections::HashMap<String, DefinitionGroup>)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static NEXT_MEMO: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The reconstruction memo's scope: ONE compilation.
///
/// A reconstructed group is built with its own fresh identity arena, exactly
/// as an unmemoized read would be — the memo saves the READING, never the
/// arena. Holding it in a guard rather than in a static is what makes the
/// boundary a thing that ends: the guard drops, the memo goes, and the next
/// compilation reads the stored source for itself.
pub struct Compilation {
    id: u64,
}

impl Compilation {
    /// Open a memo for the compilation about to run.
    pub fn open() -> Compilation {
        let id = NEXT_MEMO.with(|next| {
            let id = next.get().wrapping_add(1);
            next.set(id);
            id
        });
        MEMOS.with(|memos| {
            memos
                .borrow_mut()
                .push((id, std::collections::HashMap::new()))
        });
        Compilation { id }
    }

    fn cached(source: &str) -> Option<DefinitionGroup> {
        MEMOS.with(|memos| {
            memos
                .borrow()
                .last()
                .and_then(|(_, memo)| memo.get(source).cloned())
        })
    }

    fn remember(source: &str, group: &DefinitionGroup) {
        MEMOS.with(|memos| {
            if let Some((_, memo)) = memos.borrow_mut().last_mut() {
                memo.insert(source.to_string(), group.clone());
            }
        });
    }
}

impl Drop for Compilation {
    /// Removed BY IDENTITY, not by position: two compilations' lifetimes may
    /// overlap without nesting, and popping the top would then retire the
    /// wrong one's memo.
    fn drop(&mut self) {
        let id = self.id;
        MEMOS.with(|memos| {
            memos.borrow_mut().retain(|(open, _)| *open != id);
        });
    }
}

/// ONE CLAUSE's body, normalized with the call site's bindings in hand.
///
/// Substitution is a CST-to-AST judgment — a formal in relation position
/// becomes the supplied relation, a formal in a bound becomes the supplied
/// integer — so the bindings are supplied at the entrance rather than applied
/// to a built tree, which would have to re-decide from the AST which positions
/// were formals.
///
/// The stored source is a whole clause; what an expansion wants is the body it
/// binds. Reading the body off the reconstructed clause is what keeps the neck
/// a PARSED node rather than a substring — searching text for `:-` finds one
/// inside a string literal, a comment, or a nested definition, and the "body"
/// that follows is bytes that were never a body.
pub fn bound_body(source: &str, bindings: HoParamBindings) -> Result<Query> {
    let decls = normalized(source, Some(bindings))?.definitions;
    let Some(decl) = decls.into_iter().next() else {
        return Err(DelightQLError::parse_error(format!(
            "No definition found in source: '{}'",
            crate::pipeline::parse::truncate_for_display(source, 60)
        )));
    };
    match decl.body {
        DdlBody::Relational(query) => Ok(query),
        DdlBody::FactFunction(mode) => Ok(mode.relational_body()),
        DdlBody::Scalar(_) => Err(DelightQLError::parse_error(format!(
            "'{}' is a value rule; its body is not relational",
            crate::pipeline::parse::truncate_for_display(source, 60)
        ))),
        DdlBody::Truth(_) => Err(DelightQLError::parse_error(format!(
            "'{}' is a truth rule; its body is not relational",
            crate::pipeline::parse::truncate_for_display(source, 60)
        ))),
        // A body still awaiting substitution, handed bindings that did not
        // supply what it waits for. Saying so is the honest propagation.
        DdlBody::Deferred { source } => Err(DelightQLError::parse_error(format!(
            "the body '{}' still awaits substitution",
            crate::pipeline::parse::truncate_for_display(&source, 60)
        ))),
    }
}

/// ONE BARE BODY, normalized with the call site's bindings in hand.
///
/// A deferred payload holds the authored characters of a BODY, not of a
/// clause — the front matter was complete without them. The entrance is
/// therefore the utility one: the caller knows which of the two it is holding,
/// and the entrances OVERLAP, so nothing here guesses.
pub fn bound_relex(body_source: &str, bindings: HoParamBindings) -> Result<Query> {
    let tree = crate::pipeline::parse::query_sequence(body_source)?;
    let registry = Rc::new(crate::names::Registry::new(&[]));
    let mut normalized =
        crate::pipeline::normalize::bound_query_sequence(&tree, registry, bindings)?;
    if normalized.queries.len() != 1 {
        return Err(DelightQLError::parse_error(format!(
            "a body is one relational expression: '{}'",
            crate::pipeline::parse::truncate_for_display(body_source, 60)
        )));
    }
    Ok(normalized.queries.remove(0).query)
}

fn normalized(source: &str, bindings: Option<HoParamBindings>) -> Result<Normalized> {
    let tree = crate::pipeline::parse::definition_file(source)?;
    let registry = Rc::new(crate::names::Registry::new(&[]));
    match bindings {
        Some(bindings) => {
            crate::pipeline::normalize::bound_definition_file(&tree, registry, bindings)
        }
        None => crate::pipeline::normalize::definition_file(&tree, registry),
    }
}

/// The BODY of a stored clause, in the author's own bytes.
///
/// A definition's neck is a PARSED node, never a substring: searching text for
/// `:-` reads a neck out of a string literal, a comment, or a body's own nested
/// definition, and the "body" that follows is bytes that were never a body. The
/// grammar knows where the neck is, so it is asked.
///
/// Text that is not a definition is already a body (`x * 2`, `_( … )`) and
/// comes back unchanged — callers receive whole clauses from the catalog and
/// bare bodies from the compiler alike. A `(~~docs ~~)` annotation is the
/// definition's own `doc`, so the span already excludes it.
pub fn body_text(source: &str) -> String {
    let Ok(tree) = crate::pipeline::parse::definition_file(source) else {
        return strip_docs_block(source.trim()).to_string();
    };
    let found = crate::pipeline::syntax::walk(&tree)
        .find_map(|node| cst::EntityDefinition::cast(node.node()))
        .and_then(|definition| body_span(&tree, definition));
    match found {
        Some(body) => body,
        None => strip_docs_block(source.trim()).to_string(),
    }
}

/// One arm per definition form, so a form that gains a body cannot quietly
/// fall through to the whole-source fallback.
fn body_span(tree: &SyntaxTree, definition: cst::EntityDefinition<'_>) -> Option<String> {
    let text = |range: Option<std::ops::Range<usize>>| {
        range.map(|range| tree.source()[range].trim().to_string())
    };
    match definition {
        cst::EntityDefinition::RuleForm(rule) => match rule {
            cst::RuleForm::FoRule(rule) => text(rule.body().and_then(|b| tree.byte_range(b))),
            cst::RuleForm::HoRule(rule) => text(rule.body().and_then(|b| tree.byte_range(b))),
            cst::RuleForm::FunctionRule(rule) => text(rule.body().and_then(|b| tree.byte_range(b))),
            cst::RuleForm::ConstantRule(rule) => text(rule.body().and_then(|b| tree.byte_range(b))),
            cst::RuleForm::SigmaRule(rule) => text(rule.body().and_then(|b| tree.byte_range(b))),
            cst::RuleForm::EffectRule(rule) => text(rule.body().and_then(|b| tree.byte_range(b))),
        },
        cst::EntityDefinition::FactLike(fact) => match fact {
            cst::FactLike::FactForm(fact) => text(fact.body().and_then(|b| tree.byte_range(b))),
            cst::FactLike::HoFactForm(fact) => text(fact.body().and_then(|b| tree.byte_range(b))),
        },
        // A fact function's arms ARE its body, and it has no body field to
        // point at: the whole form is the answer.
        cst::EntityDefinition::FactFunction(function) => text(tree.byte_range(function)),
        cst::EntityDefinition::EdgeDeclaration(edge) => {
            text(edge.body().and_then(|b| tree.byte_range(b)))
        }
    }
}

/// Strip a leading `(~~docs … ~~)` from body text the grammar could not place.
fn strip_docs_block(text: &str) -> &str {
    let trimmed = text.trim_start();
    if let Some(rest) = trimmed.strip_prefix("(~~docs") {
        if let Some(end) = rest.find("~~)") {
            return rest[end + 3..].trim_start();
        }
    }
    trimmed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::reconstruct;
    use crate::pipeline::asts::core::definitions::{HoParam, Neck};
    use crate::pipeline::asts::core::{DomainExpression, FunctionApplication};
    use crate::pipeline::asts::ddl::DdlBody;
    use crate::pipeline::asts::ddl::{entity_type_id, DefKind};

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
        assert_eq!(def.neck, Neck::Bind);

        let params = def.params();
        assert_eq!(params.len(), 1);
        assert_eq!(scalar_param(&params[0]), ("x".to_string(), false));

        // Body should be a scalar (DomainExpression)
        let expr = def
            .as_out_value()
            .and_then(crate::pipeline::asts::core::OutValue::domain)
            .expect("expected scalar body");
        match expr {
            DomainExpression::Application(FunctionApplication::Infix(infix)) => {
                assert_eq!(infix.operator, crate::pipeline::asts::vocabulary::BinOp::Mul);
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
        assert_eq!(def.neck, Neck::Bind);
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
    fn test_build_persistent_neck() {
        let source = "cached:(x) := x + 1";
        let defs = reconstruct::group(source).unwrap().into_clauses();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].neck, Neck::Assign);
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
        let expr = def
            .into_out_value()
            .and_then(crate::pipeline::asts::core::OutValue::into_domain)
            .expect("expected scalar body");
        match &expr {
            DomainExpression::Application(FunctionApplication::Infix(infix)) => {
                assert_eq!(infix.operator, crate::pipeline::asts::vocabulary::BinOp::Mul);
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
    /// A higher-order template whose text the body parser cannot read until
    /// its parameters are substituted still assembles into a group: a
    /// subject, a kind, an arity, and a head are all written LEFT of the
    /// neck, so reading them waits on no argument.
    #[test]
    fn a_deferred_body_still_assembles_its_group() {
        // The premise this test rests on: a bound naming a scalar formal has
        // no integer to be until a call site supplies one. If normalization
        // learns to read it unbound, the assertion fails loudly rather than
        // the test quietly ceasing to exercise the deferral.
        const TEMPLATE: &str = "T(*), #<n";

        let one = reconstruct::group(&format!("pick(T(*), n)(a, b) :- {TEMPLATE}"))
            .expect("a deferred body still builds a group");
        assert!(
            matches!(one.first().body, DdlBody::Deferred { .. }),
            "the premise: the body defers"
        );
        assert_eq!(one.name(), "pick");
        assert_eq!(one.kind(), DefKind::HoView);
        assert_eq!(one.bound_param_names().len(), 2, "the fronts are complete");

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
    /// a name: mixed kinds, disagreeing arity, and the head algebra all
    /// refuse at build.
    #[test]
    fn the_group_door_runs_the_clause_laws() {
        let mixed_kind = reconstruct::group("foo:(x) :- x + 1\nfoo(x) :- x > 0")
            .expect_err("a function and a sigma are not one definition");
        assert_eq!(
            mixed_kind.error_uri(),
            "delightql-error://semantic/ddl/head/mixed_kind"
        );

        let param_arity = reconstruct::group(
            "empty(column) :- null = column\nempty(column, other) :- trim:(column) = other",
        )
        .expect_err("clauses must agree on how many positions there are");
        assert_eq!(
            param_arity.error_uri(),
            "delightql-error://semantic/ddl/head/param_arity"
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
        let params = group.params();
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
        assert_eq!(def.neck, Neck::Bind);

        {
            let params = def.params();
            assert_eq!(params.len(), 1);
            assert_eq!(scalar_param(&params[0]), ("column".to_string(), false));
        }

        // A sigma rule's body is a TRUTH, and the carrier says so.
        assert!(def.as_truth_expr().is_some());
        assert!(def.as_out_value().is_none());
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
            crate::pipeline::asts::ddl::entity_type_id(group.kind(), group.context()),
            9
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
            crate::pipeline::asts::ddl::entity_type_id(
                reconstruct::group("foo:(x) :- x + 1").unwrap().kind(),
                reconstruct::group("foo:(x) :- x + 1").unwrap().context()
            ),
            1,
            "foo:(x) should be Function"
        );
        assert_eq!(
            crate::pipeline::asts::ddl::entity_type_id(
                reconstruct::group("foo(x) :- x > 0").unwrap().kind(),
                reconstruct::group("foo(x) :- x > 0").unwrap().context()
            ),
            9,
            "foo(x) should be SigmaPredicate"
        );
    }

    #[test]
    fn test_build_fact_definition() {
        let source = r#"person(0, "Gusti", "Parlor")"#;
        let defs = reconstruct::group(source).unwrap().into_clauses();
        assert_eq!(defs.len(), 1);

        let def = &defs[0];
        // A fact writes NO neck; what it does is ASSIGN materialized data.
        assert_eq!(def.neck, Neck::Assign);
        // Body should be relational (anonymous table)
        assert!(matches!(def.body, DdlBody::Relational(_)));
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
/// Reconstruction is not a tooling entrance: `group`, `bound_body` and their
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

        for (entrance, error) in [
            ("clauses", super::clauses(&source).expect_err("clauses")),
            ("group", super::group(&source).expect_err("group")),
            (
                "bound_body",
                super::bound_body(&source, Default::default()).expect_err("bound_body"),
            ),
            (
                "bound_relex",
                super::bound_relex(&bare, Default::default()).expect_err("bound_relex"),
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

/// FACT ELABORATION (R4.2.7): a fact elaborates once, during definition
/// assembly, into the ordinary ground relational clause shape — pinned at
/// the same door the catalog's stored sources re-enter, so registration and
/// reconstruction cannot drift.
#[cfg(test)]
mod fact_elaboration_pins {
    use super::*;
    use crate::pipeline::asts::ddl::{DdlBody, DefKind, Supply};

    fn canonical(group: &DefinitionGroup) -> Vec<String> {
        group
            .canonical_names()
            .expect("an elaborated fact group has a settled heading")
            .iter()
            .map(|name| name.to_string())
            .collect()
    }

    /// A standard fact becomes one ground-headed clause per row over a unit
    /// body, and a fact-only definition's unoffered positions receive the
    /// canonical fact name `subject|N|`.
    #[test]
    fn a_standard_fact_elaborates_per_row_with_canonical_names() {
        let built = group("person(0, \"Gusti\")\nperson(1, \"Diane\")").unwrap();
        assert_eq!(built.kind(), DefKind::Fact);
        assert_eq!(built.clauses().len(), 2);
        assert_eq!(canonical(&built), vec!["person|1|", "person|2|"]);
        for clause in built.clauses() {
            let items = clause.head.items.listed().expect("a ground head");
            assert_eq!(items.len(), 2);
            assert!(items
                .iter()
                .all(|item| matches!(item.supply, Supply::Ground(_))));
            assert!(matches!(clause.body, DdlBody::Relational(_)));
        }
    }

    /// A multi-row standard fact splits per ROW: each row is its own clause,
    /// so a duplicate row is a duplicate clause and stays a duplicate proof
    /// through the ordinary UNION ALL combination.
    #[test]
    fn multi_row_and_duplicate_rows_are_clauses_and_proofs() {
        let built = group(r#"b("foo","X"; "bar","Y")"#).unwrap();
        assert_eq!(built.clauses().len(), 2);

        let duplicates = group("d(7)\nd(7)").unwrap();
        assert_eq!(duplicates.clauses().len(), 2, "two proofs of one row");
    }

    /// Heading offers settle once for the complete definition: an offer
    /// names the position and a bare value abstains — the RULINGS example.
    #[test]
    fn offers_settle_once_and_abstentions_do_not_defeat_them() {
        let built = group("f(1 as a, 2 as b)\nf(3, 4)").unwrap();
        assert_eq!(canonical(&built), vec!["a", "b"]);
    }

    /// A stacked fact stays ONE clause: the header names the positions, the
    /// head plumbs them, and the table remains the body.
    #[test]
    fn a_stacked_fact_is_one_clause_plumbing_its_header() {
        let built = group(r#"employee(Id, Name --- 0, "Gusti"; 1, "Diane")"#).unwrap();
        assert_eq!(built.kind(), DefKind::Fact);
        assert_eq!(built.clauses().len(), 1);
        assert_eq!(canonical(&built), vec!["Id", "Name"]);
        let items = built.clauses()[0].head.items.listed().expect("plumb head");
        assert!(items.iter().all(|item| item.supply.is_reference()));
    }

    /// Mixed fact and relational clauses are the one ruled kind union: the
    /// group is a relational definition and rule names win where facts
    /// abstain.
    #[test]
    fn mixed_fact_and_rule_clauses_are_one_relational_definition() {
        let built = group("b(\"seed\", \"X\")\nb(tag, x) :- _(tag, x ---- \"r\", \"Y\")").unwrap();
        assert_eq!(built.kind(), DefKind::View);
        assert_eq!(canonical(&built), vec!["tag", "x"]);
        // Any OTHER mix is still two kinds under one spelling.
        for other in ["b(1)\nb:(x) :- x + 1", "b(1)\nb(x) :- x > 0"] {
            let err = group(other).unwrap_err();
            assert_eq!(
                err.error_uri(),
                "delightql-error://semantic/ddl/head/mixed_kind"
            );
        }
    }

    /// The refusal identities: offer disagreement, width disagreement, and
    /// the Ground-Position rule — which the FACT spelling authenticates and
    /// the rule spelling does not.
    #[test]
    fn the_fact_refusal_identities() {
        let disagreement = group("g(1 as a)\ng(2 as z)").unwrap_err();
        assert_eq!(
            disagreement.error_uri(),
            "delightql-error://semantic/ddl/head/name_conflict"
        );

        let width = group("w(1, 2)\nw(3)").unwrap_err();
        assert_eq!(
            width.error_uri(),
            "delightql-error://semantic/ddl/head/arity"
        );

        // Fact syntax authenticates its positions…
        assert!(group(r#"b("c", "d")"#).is_ok());
        // …and the ordinary rule spelling of the same data still refuses.
        let rule = group(r#"b("c" as c, "d") :- _(1)"#).unwrap_err();
        assert_eq!(
            rule.error_uri(),
            "delightql-error://semantic/ddl/head/unnamed_ground_position"
        );
    }

    /// Identity is the identifier law's: an unstropped spelling folds to one
    /// name; a strop is a different name and conflicts.
    #[test]
    fn offers_agree_by_identifier_not_characters() {
        let folded = group("r(1 as TAG)\nr(2 as tag)").unwrap();
        assert_eq!(folded.canonical_names().unwrap().len(), 1);

        let stropped = group("q(1 as `Tag`)\nq(2 as tag)").unwrap_err();
        assert_eq!(
            stropped.error_uri(),
            "delightql-error://semantic/ddl/head/name_conflict"
        );
        assert!(stropped.to_string().contains("`Tag`"));
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
