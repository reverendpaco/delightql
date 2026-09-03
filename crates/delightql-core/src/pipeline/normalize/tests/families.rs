// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! One discriminating assertion per semantic family: the pairs that must NOT
//! normalize alike, and the pairs that must.

use super::support::*;
use crate::pipeline::asts::core::*;
use crate::pipeline::asts::ddl::{DdlBody, DefKind, DefSubject, Fixpoint};

// ---------------------------------------------------------------------
// Entrances
// ---------------------------------------------------------------------

/// The branches OVERLAP: identical bytes are a fact in one entrance and a
/// query in the other. Nothing guesses; the caller names the category.
#[test]
fn the_same_bytes_are_a_fact_or_a_query_by_entrance() {
    let as_query = queries("f(1, 2)");
    assert_eq!(as_query.queries().count(), 1);
    assert!(as_query.definitions().next().is_none());

    let as_file = file("f(1, 2)");
    assert!(as_file.queries().next().is_none());
    assert_eq!(as_file.definitions().count(), 1);
    assert_eq!(
        as_file
            .definitions()
            .nth(0)
            .expect("a definition")
            .front
            .kind,
        DefKind::Fact
    );
}

/// A canonical file declaring nothing declares nothing.
#[test]
fn an_empty_canonical_file_is_lawful() {
    let empty = file("");
    assert!(empty.queries().next().is_none());
    assert!(empty.definitions().next().is_none());
}

/// `?-` is the sole top-level-goal marker: it names the category and carries
/// nothing into the AST.
#[test]
fn a_top_level_goal_is_its_body() {
    let goal = file("?- users(*)");
    assert_eq!(goal.queries().count(), 1);
    assert_eq!(
        lispy(&goal.queries().nth(0).expect("a goal").query),
        lispy(&query("users(*)"))
    );
}

// ---------------------------------------------------------------------
// Access — the one authority
// ---------------------------------------------------------------------

/// `()` is INCHOATE where `*` ACTIVATES. The two stay distinct values; a
/// consumer that needs the difference matches the variant.
#[test]
fn inchoate_and_activated_reads_stay_apart() {
    assert!(matches!(access_of("users()"), Access::Unasked));
    assert!(matches!(access_of("users(*)"), Access::All));
}

/// A dequalifying run says what the mention's parens say, so it folds INTO
/// the access rather than standing beside it as a pipe.
#[test]
fn the_dequalifying_run_folds_into_the_access() {
    assert!(matches!(access_of("users(*.(a, b))"), Access::Dequalify(_)));
    assert!(matches!(access_of("users(.*)"), Access::DequalifyAll));
    let Access::Dequalify(columns) = access_of("users(*.(a, b))") else {
        panic!("expected a dequalifying access");
    };
    assert_eq!(
        columns.iter().map(|c| c.as_str()).collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    // The step names a COLUMN, and a strop is part of that name — the carrier
    // holds what the author wrote, so the lvar this step renames onto is the
    // one they spelled.
    let Access::Dequalify(written) = access_of("users(*.(`Mixed`))") else {
        panic!("expected a dequalifying access");
    };
    assert!(
        written[0].is_stropped() && written[0].as_str() == "Mixed",
        "the dequalifying step keeps the strop: {written:?}"
    );
}

/// THE RUN IS THE SAME RUN OUTSIDE THE PARENS. `users(*.(a))`, `users()*.(a)`
/// and `users() * .(a)` are one query, so they must reach one AST — a second
/// reading outside the parens is a second answer to the same question.
#[test]
fn the_run_reads_alike_inside_and_outside_the_parens() {
    for outside in ["users()*.(a, b)", "users(*) .(a, b)", "users() .(a, b)"] {
        assert_eq!(
            lispy(&query(outside)),
            lispy(&query("users(*.(a, b))")),
            "{outside} should read as the interior run"
        );
    }
    assert_eq!(
        lispy(&query("users(*) .*")),
        lispy(&query("users(.*)")),
        "the every-name run reads alike too"
    );
}

/// `,` puts a mention in MEMBER position, and the run written after it is
/// that member's own. Read at the outer chain the member would stay unasked
/// and the step would face a heading holding the shared name twice.
#[test]
fn a_run_after_a_member_is_the_member_s_own() {
    assert_eq!(
        lispy(&query("users(*), orders() .(status)")),
        lispy(&query("users(*), orders(.(status))"))
    );
    assert_eq!(
        lispy(&query("users(*), orders() *.(status)")),
        lispy(&query("users(*), orders(*.(status))"))
    );
}

/// ONE ACCESS PER PARENS. A caller pattern has already said which dimensions
/// the mention asks for, so the step after it is an access step on its RESULT
/// and must not overwrite the pattern.
///
/// ONE CARRIER, BOTH POSITIONS: the step holds the same `Access` value the
/// mention would have absorbed, so `users(*.(name))` and this reach one
/// authority. The absorbed spelling is asserted alongside to pin that.
#[test]
fn a_run_after_a_caller_pattern_is_a_step_on_its_result() {
    assert!(matches!(
        access_of("users(id, name, _).(name)"),
        Access::Slots(_)
    ));
    let stepped = query("users(id, name, _).(name)");
    assert!(shows(&stepped, "continuation:access"));
    assert!(shows(&stepped, "access:dequalify"));
    // The absorbed road produces the same access, in the mention instead.
    assert!(shows(&query("users(*.(name))"), "access:dequalify"));
}

/// The alias renames THE RELATION, and the interior reads that same
/// relation — so a continuation inside the parens addresses it by the new
/// name, exactly as it would by the reference when none was written.
#[test]
fn an_alias_renames_the_relation_the_interior_reads() {
    assert!(shows(
        &query("users(*) as u, orders(, o.user_id = u.id |> (id)) as o"),
        "(alias \"o\")"
    ));
    // The rename reaches the interior's own read, not only the wrapper: two
    // occurrences of the name, the derived table's and the base read's.
    let normalized = lispy(&query("orders(, orders.id > 1 |> (id)) as o"));
    assert_eq!(
        normalized.matches("(alias \"o\")").count(),
        2,
        "the wrapper and the read it opened both answer to the name: {normalized}"
    );
}

/// A caller pattern is one slot per dimension: a bare name BINDS, `_` binds
/// nothing, a ground term CONSTRAINS.
#[test]
fn a_caller_pattern_classifies_its_slots_once() {
    let Access::Slots(slots) = access_of("users(a, _, 30)") else {
        panic!("expected slots");
    };
    assert_eq!(slots.len(), 3);
    assert!(slots.get(0).expect("first").binder().is_some());
    assert!(matches!(slots.get(1).expect("second"), Slot::Anon));
    assert!(slots.get(2).expect("third").ground().is_some());
}

/// SNEAKY PARENTHESES: a SHAPING interior is a derived table, and the fold
/// stops there rather than pretending the parens were an access.
#[test]
fn a_shaping_interior_becomes_a_derived_table() {
    let query = query("users(, age > 3)");
    let chain = &query.body;
    assert!(matches!(
        chain.head().form(),
        GroundForm::Reference(Relation::InnerRelation { .. })
    ));
}

fn access_of(source: &str) -> Access<Unresolved> {
    let query = query(source);
    let chain = query.body;
    assert!(
        matches!(
            chain.head().form(),
            GroundForm::Reference(Relation::Ground { .. })
        ),
        "expected a ground read, got {:?}",
        chain.head()
    );
    chain
        .head_access()
        .cloned()
        .expect("a ground read carries its own access")
}

// ---------------------------------------------------------------------
// Value position
// ---------------------------------------------------------------------

/// The citation is the OTHER nullary: it normalizes to the zero-argument
/// application and is never ground. After this, nothing can tell `:pi` from
/// `pi:()`.
#[test]
fn a_citation_is_the_zero_argument_application() {
    assert_eq!(
        lispy(&query("users(*) |> (:pi)")),
        lispy(&query("users(*) |> (pi:())"))
    );
}

/// ONE TEMPLATE PARSE, classified once by CONTENT: zero interpolations is a
/// ground string, and `template` is non-ground by construction.
#[test]
fn a_zero_interpolation_template_is_a_ground_string() {
    assert!(shows(
        &query("users(*) |> (:\"hi\")"),
        "(literal_value:string \"hi\")"
    ));
    assert!(shows(
        &query("users(*) |> (:\"x{n}\")"),
        "domain_expression:function:string_template"
    ));
}

/// Parens are ADMISSION: an operand derives no infix form, so nesting
/// re-enters only through them and there is no precedence to apply.
#[test]
fn infix_nests_only_through_parens() {
    assert!(shows(
        &query("users(*) |> ((a + b) * c)"),
        "domain_expression:function:infix"
    ));
    // `a + b * c` has no derivation: NO PEMDAS, structurally.
    let tree =
        crate::pipeline::syntax::Parser::new().parse_query_sequence("users(*) |> (a + b * c)");
    assert!(tree.has_defects(), "PEMDAS must have no derivation");
}

/// The two anaphors instantiate per level as DIFFERENT carriers, so a
/// relational landing can never be mistaken for a value-level hole.
#[test]
fn the_two_anaphors_are_two_carriers() {
    // Value level: the flowing value inside a callable is the open leaf.
    assert!(shows(
        &query("users(*) |> $(upper:(@))(n)"),
        "domain_hole:composition_input"
    ));
    // Relational level: the landing an invocation's argument row spells is
    // SPENT into an ordinary relation argument at the formal it names.
    assert!(shows(
        &query("users(*) |> f(@, b(*))(*)"),
        "ho_argument:relation"
    ));
}

/// `=` and `!=` are the null-safe pair, and the ONLY authored equality
/// glyphs: the engine's own answer is the prelude sigma predicate
/// (`+sql_eq(l, r)` / `+sql_ne(l, r)`), an application that normalizes as a
/// call and never as a comparison operator. The retired `==` / `!==` are not
/// tokens — the grammar refuses them and the parse diagnosis teaches the two
/// roads — so no decoder arm exists for this test to reach.
#[test]
fn the_null_safe_pair_is_the_authored_comparison_vocabulary() {
    assert!(shows(
        &query("users(*), a = 1"),
        "(operator IS NOT DISTINCT FROM)"
    ));
    assert!(shows(
        &query("users(*), a != 1"),
        "(operator IS DISTINCT FROM)"
    ));
    let sigma = query("users(*), +sql_eq(a, 1)");
    assert!(shows(&sigma, "truth_expression:sigma"));
    assert!(!shows(&sigma, "(operator ="));
    let sigma = query("users(*), +sql_ne(a, 1)");
    assert!(shows(&sigma, "truth_expression:sigma"));
    assert!(!shows(&sigma, "(operator !="));
    for retired in ["users(*), a == 1", "users(*), a !== 1"] {
        let tree = crate::pipeline::syntax::Parser::new().parse_query_sequence(retired);
        assert!(tree.has_defects(), "the grammar admitted {retired:?}");
    }
}

// ---------------------------------------------------------------------
// Truth position
// ---------------------------------------------------------------------

/// EXISTENCE IS TRUTH: `+rel(…)` in comma position RESTRICTS the current
/// relation. It is a truth expression, never a relational carrier — no
/// member, no bag arm.
#[test]
fn existence_in_comma_position_restricts() {
    let query = query("users(*), +orders(, a = 1)");
    let chain = &query.body;
    assert_eq!(chain.steps().len(), 1);
    assert!(matches!(
        chain.steps()[0].form(),
        Continuation::Restrict { .. }
    ));
    assert!(shows(&query, "truth_expression:existence"));
}

/// The value-position spelling reaches THE SAME carrier through the
/// truth-to-value crossing: one surface, one carrier.
#[test]
fn existence_in_value_position_crosses_to_the_same_carrier() {
    assert!(shows(
        &query("users(*) |> (+orders(, a = 1))"),
        "truth_expression:existence"
    ));
}

/// Existence cannot serve as the relational peer that completes a leading
/// outer join: `?` waits for a RELATION.
#[test]
fn existence_does_not_complete_a_leading_outer() {
    let tree = crate::pipeline::syntax::Parser::new().parse_query_sequence("a?(*), +b(, x = 1)");
    assert!(
        tree.has_defects(),
        "a truth member cannot complete a leading outer join"
    );
}

/// Polarity is DATA, one carrier: the same node, read once.
#[test]
fn polarity_is_read_once() {
    assert!(shows(&query("users(*), +o(, a = 1)"), "polarity:positive"));
    assert!(shows(
        &query("users(*), \\+o(, a = 1)"),
        "polarity:negative"
    ));
}

// ---------------------------------------------------------------------
// Spec position
// ---------------------------------------------------------------------

/// THE SINGLETON PIPE is sugar for the zero-key group, and it reaches the
/// group through ONE road: the two spellings are the same operator.
#[test]
fn the_singleton_pipe_is_the_zero_key_group() {
    assert_eq!(
        lispy(&query("users(*) ~> sum:(x)")),
        lispy(&query("users(*) |> %( ~> sum:(x))"))
    );
}

/// A group with no reduction is a DISTINCT; with one it is a reduction. The
/// `~>` decides, and it is the only thing that does.
#[test]
fn the_reduction_sigil_decides_the_group() {
    assert!(shows(&query("users(*) |> %(g)"), "group_spec:distinct"));
    assert!(shows(
        &query("users(*) |> %(g ~> sum:(x))"),
        "group_spec:reduce"
    ));
}

/// RETENTION DECIDES POSITION: the postfix form keeps the context, and the
/// post-pipe one keeps the payload alone — the same expansion, then a
/// projection.
#[test]
fn drill_keeps_context_and_narrowing_keeps_payload() {
    assert_eq!(
        lispy(&query("users(*) |> .t(*)")),
        lispy(&query("users(*) .t(*) |> (t.*)"))
    );
}

/// `as f` on a STAGE names the stage's output; on a bare head there is no
/// stage yet, so it names the mention.
#[test]
fn a_stage_name_lands_on_the_stage_it_names() {
    assert!(shows(&query("users(*) |> (x) as s"), "(named \"s\")"));
    assert!(shows(&query("users(*) as s"), "(alias \"s\")"));
    // The name lands on the stage it stands AFTER — a member's output is the
    // member's own relation, not the chain's head.
    let chain = query("a(*), b(*) as s").body;
    let Some(Continuation::Member { rhs, .. }) =
        chain.continuations().last().map(|step| step.form())
    else {
        panic!("expected a member");
    };
    assert_eq!(
        rhs.as_read_relation().and_then(|r| match r {
            Relation::Ground { mention, .. } => mention.alias().map(ToString::to_string),
            _ => None,
        }),
        Some("s".to_string())
    );
    // A restriction publishes no stage, and naming nothing refuses rather
    // than being dropped.
    assert!(refusal("a(*), x > 1 as s").contains("names nothing"));
    // An existence probe DOES publish one: the relation it probes.
    assert!(shows(&query("a(*), +b(, x = 1) as r"), "(alias \"r\")"));
}

/// A bound has ONE home — the comma member — and it is its OWN continuation
/// there, not a restriction carrying a condition that happens not to be a
/// truth. The AST stores the bound, not the spelling it arrived by.
#[test]
fn a_bound_stores_the_bound() {
    assert!(shows(&query("users(*), #<5"), "continuation:bound"));
    assert!(!shows(&query("users(*), #<5"), "continuation:restrict"));
}

// ---------------------------------------------------------------------
// Effects
// ---------------------------------------------------------------------

/// The pipe is SUBSTITUTION: `q |> f!(acc)` is the same call as `f!(acc, q)`,
/// and the landing is spent INTO THE ROW — one group after a pipe is receipt
/// access.
///
/// The source lands as its own member kind. That kind is the whole record of
/// where the relation came from: there is no index beside the row for a later
/// phase to keep in step with it, and nothing can turn the member into an
/// ordinary argument without saying so.
#[test]
fn a_piped_effect_substitutes_its_source() {
    let query = query("users(*) |> stdout!(*)");
    assert!(shows(&query, "relation:functor_call"));
    assert!(shows(&query, "ho_argument:landed"));
    assert!(shows(&query, "(access (access:all))"));
    // The receipt group contributed the access and NOT a second argument,
    // and nothing put a second, authored relation beside the landed one.
    assert!(!shows(&query, "ho_argument:value"));
    assert!(!shows(&query, "ho_argument:relation"));
}

/// A DIRECTLY WRITTEN relation actual is NOT a landing, and the two are told
/// apart by the member rather than by anything a consumer must remember.
#[test]
fn a_written_relation_actual_is_not_a_landing() {
    let direct = query("f(users(*))(*)");
    assert!(shows(&direct, "ho_argument:relation"));
    assert!(!shows(&direct, "ho_argument:landed"));
}

/// Two groups in ground position are (parameters)(receipt access) — read by
/// POSITION, never by what is in them.
#[test]
fn two_ground_groups_are_parameters_then_receipt() {
    let query = query("stdout!(\"x\")(*)");
    assert!(shows(&query, "ho_argument:value"));
    assert!(shows(&query, "(access (access:all))"));
    // The receipt group is the access; it leaves no trailing operator.
    let chain = &query.body;
    assert!(chain.steps().is_empty());
}

/// `!!` is call-site evidence on the MENTION: there is no entity named
/// `emp!!`.
#[test]
fn a_mutation_marker_rides_on_the_mention() {
    assert!(shows(
        &query("emp!!(*), a > 1 |> update!(emp(*))(*)"),
        "(mutation_target true)"
    ));
}

/// THE UNWRAP PIPE is a pipe FORM: `Q !> S ≡ Q |> S |> .returned(*)`.
#[test]
fn the_unwrap_pipe_is_the_long_spelling() {
    assert_eq!(
        lispy(&query("users(*) !> stdout!(*)")),
        lispy(&query("users(*) |> stdout!(*) |> .returned(*)"))
    );
}

// ---------------------------------------------------------------------
// Definitions
// ---------------------------------------------------------------------

/// Rule bodies come in three positions and the GRAMMAR sorted them, so the
/// declared kind is never re-derived from the body's Rust type.
#[test]
fn the_head_form_declares_the_kind() {
    assert_eq!(definition("v(*) :- users(*)").front.kind, DefKind::View);
    assert_eq!(definition("f:(x) :- (x * 2)").front.kind, DefKind::Function);
    assert_eq!(definition("pi :- 3.14").front.kind, DefKind::Function);
    assert_eq!(definition("p(x) :- x > 1").front.kind, DefKind::Sigma);
    assert_eq!(definition("h(T(*))(*) :- T(*)").front.kind, DefKind::HoView);
    assert_eq!(
        definition("d!(*) :- stdout!(*)").front.kind,
        DefKind::Effect
    );
    assert_eq!(definition("f(1, 2)").front.kind, DefKind::Fact);
}

/// A CLAUSE BODY IS A COMPLETE EXPRESSION OF ITS CATEGORY (FN.43). The
/// relational rule's body is a `relex` — a let block and the chain it feeds —
/// and the effect rule's is `effrelex`, its effectual twin. A labelled CTE was
/// spellable in every other effect position and not here, which is a shape
/// accident rather than a distinction the effect algebra draws.
#[test]
fn an_effect_rule_body_carries_its_let_block() {
    // A PURE binding feeding an effectual body — the block itself is one
    // production and carries both kinds.
    let pure_binding = definition("main!(*) :-\n  x(*) : q\n  q(*) |> log!(*)");
    assert_eq!(pure_binding.front.kind, DefKind::Effect);
    assert!(lispy_body(&pure_binding).contains("cte_binding"));

    // A standard effect CTE head reaches the same block.
    assert_eq!(
        definition("main!(*) :-\n  q!(*) : x(*) |> log!(*)\n  q!(*)")
            .front
            .kind,
        DefKind::Effect
    );

    // The BODY still effectuates: a block over a purely pure chain is not an
    // effect rule, and admitting the block did not admit the mixture.
    assert!(file_refusal("main!(*) :-\n  x(*) : q\n  q(*)").contains("Parse"));

    // A rule with no block is untouched: its query carries no bindings.
    assert!(!lispy_body(&definition("main!(*) :- s!(*)")).contains("cte_binding"));
}

/// A definition's documentation reaches the clause as its own payload, from
/// the doc slot and from nowhere else. The delimiters are not part of it: what
/// travels to the catalog is the text the author wrote between them.
#[test]
fn a_definition_doc_becomes_the_clauses_documentation() {
    assert_eq!(
        definition("v(*) :- (~~docs Users aged 65 or older. ~~) users(*), age >= 65").doc,
        Some("Users aged 65 or older.".to_string())
    );
    assert_eq!(
        definition("f:(x) :- (~~docs Multiplies the input by two. ~~) (x * 2)").doc,
        Some("Multiplies the input by two.".to_string())
    );
    // Prose is prose: `*`, `/` and `!` inside the body are text, not syntax.
    assert_eq!(
        definition("v(*) :- (~~docs Uses users(*) and f!/g at 100% ~~) users(*)").doc,
        Some("Uses users(*) and f!/g at 100%".to_string())
    );
    assert_eq!(definition("v(*) :- users(*)").doc, None);
    // The smart comment is the OTHER documentation form and stays distinct:
    // it attaches by position and is not the clause's own doc payload.
    assert_eq!(definition("v(*) :- (/* a note */) users(*)").doc, None);
}

/// `doc_slot = (definition_doc | annotation)+`, and BOTH inhabitants are
/// answered. An annotation there is not documentation — it reaches the
/// collector it reaches anywhere else, independently of the doc payload.
#[test]
fn an_annotation_in_a_doc_slot_reaches_its_own_collector() {
    let dangered = file("v(*) :- (~~danger://cardinality/cartesian ~~) users(*)");
    assert_eq!(dangered.declared.dangers.len(), 1);
    assert_eq!(
        dangered.declared.dangers[0].uri,
        "delightql-danger://cardinality/cartesian"
    );
    // …and the annotation is not mistaken for the clause's documentation.
    assert_eq!(
        dangered.definitions().nth(0).expect("a definition").doc,
        None
    );

    let configured = file("v(*) :- (~~config://generation/rule/inlining/view ~~) users(*)");
    assert_eq!(configured.declared.options.len(), 1);

    // Both inhabitants at once, each still itself.
    let both =
        file("v(*) :- (~~docs What it is. ~~) (~~danger://cardinality/cartesian ~~) users(*)");
    assert_eq!(both.declared.dangers.len(), 1);
    assert_eq!(
        both.definitions().nth(0).expect("a definition").doc,
        Some("What it is.".to_string())
    );
}

/// The retired assertion annotation has no derivation in any annotation slot.
#[test]
fn the_retired_assertion_annotation_has_no_derivation() {
    let refused = crate::pipeline::syntax::Parser::new().parse_definition_file(
        "v(*) :- (~~assert ~> count:(*) as c, c > 0 |> `exists`(*) ~~) users(*)",
    );
    assert!(refused.has_defects(), "a doc slot takes no assertion");

    let anchored = crate::pipeline::syntax::Parser::new().parse_definition_file(
        "v(*) :- users(*) (~~assert ~> count:(*) as c, c > 0 |> `exists`(*) ~~)",
    );
    assert!(
        anchored.has_defects(),
        "a relation slot takes no retired annotation"
    );
}

/// ONE definition document per slot, structurally: a second has no derivation,
/// so nothing counts them and no consumer can forget to.
#[test]
fn a_definition_carries_at_most_one_document() {
    let refused = crate::pipeline::syntax::Parser::new()
        .parse_definition_file("v(*) :- (~~docs one ~~) (~~docs two ~~) users(*)");
    assert!(refused.has_defects(), "a slot carries one document");
}

/// FACT ELABORATION: a fact is not a distinct body kind — it elaborates into
/// an ordinary ground relational body.
#[test]
fn a_fact_elaborates_into_a_relational_body() {
    let clause = definition("f(a, b ---- 1, 2)");
    let DdlBody::Relational(query) = &clause.body else {
        panic!("a fact body is relational");
    };
    let GroundForm::Literal(table) = query.body.head().form() else {
        panic!("a fact body is an anonymous table");
    };
    assert_eq!(table.table.body.rows.len(), 1);
    assert_eq!(
        table.table.body.header.as_ref().map(TabularRow::len),
        Some(2),
        "the header row is a slot row"
    );
}

/// A SPARSE FILL IS A DATUM WHEREVER A HEADER MAY DECLARE ONE SPARSE. A fact
/// declares its columns with the same `header_row` an anonymous table uses, so
/// a `?` means the same thing in both and the same fill supplies it: an
/// omitted sparse column is NULL, a filled one carries the constant the fill
/// named, and ONE algorithm assembles both.
#[test]
fn a_fact_row_fills_a_sparse_column() {
    let source = "config(kind, value, note? ---- \"a\", 1 ; \"b\", 2, _(note @ \"why\"))";
    let clause = definition(source);
    let DdlBody::Relational(fact_query) = &clause.body else {
        panic!("a fact body is relational");
    };
    let GroundForm::Literal(table) = fact_query.body.head().form() else {
        panic!("a fact body is an anonymous table");
    };
    assert_eq!(
        table.table.body.header.as_ref().map(TabularRow::len),
        Some(3)
    );
    assert_eq!(table.table.body.rows.len(), 2);
    // The row that omitted the sparse column is still the heading's width.
    assert_eq!(table.table.body.rows.first().len(), 3);
    assert!(matches!(
        table.table.body.rows.first().0.get(2).unwrap().value(),
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Null))
    ));
    assert!(matches!(
        table.table.body.rows.get(1).unwrap().0.get(2).unwrap().value(),
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(text))) if text == "why"
    ));

    // THE ALGORITHM IS ONE: the anonymous relation spelling the same table
    // builds the same rows, and neither carries a second reading of `?`.
    let anon = query("_(kind, value, note? ---- \"a\", 1 ; \"b\", 2, _(note @ \"why\"))");
    assert_eq!(lispy_body(&clause), lispy(&anon));

    // The existing authorities keep answering. An unknown column, a
    // non-sparse column, a duplicate, and an arity-invalid fill each refuse
    // under the identity they already had.
    assert!(file_refusal("config(key, note? ---- \"a\", _(other @ 1))")
        .contains("is not a sparse column"));
    assert!(file_refusal("config(key, note ---- \"a\", _(note @ 1))")
        .contains("filled where no column is sparse"));
    assert!(
        file_refusal("config(key, note? ---- \"a\", _(note @ 1), _(note @ 2))")
            .contains("Duplicate sparse fill")
    );
    assert!(
        file_refusal("config(key, note? ---- \"a\", _(note @ 1, 2))")
            .contains("names 1 column(s) and supplies 2 value(s)")
    );
}

/// A FACT-FUNCTION'S INPUT ARMS ARE GROUND MATCH ROWS. It stays both an
/// inspectable fact table and a callable mode; a condition has no derivation
/// in an arm, and the SEARCHED form is an ordinary function rule whose body is
/// a searched case.
#[test]
fn a_fact_function_matches_and_the_searched_form_is_a_function_rule() {
    let matched = definition("grade(score -> letter ---- 90 -> \"A\"; _ -> \"F\")");
    assert_eq!(matched.front.kind, DefKind::FactFunction);

    // A condition does not derive in an arm — a parse refusal, not a builder
    // check a consumer could forget to run.
    assert!(
        file_refusal("grade(score -> letter ---- score > 90 -> \"A\"; _ -> \"F\")")
            .starts_with("Parse")
    );

    // The canonical searched spelling, and it is an ordinary function.
    let searched =
        definition("grade:(score) :- _:(score > 90 -> \"A\"; score > 80 -> \"B\"; _ -> \"F\")");
    assert_eq!(searched.front.kind, DefKind::Function);
    // SEARCHED, not anchored: the HEADER classifies, and the anchored shape
    // is a different variant no arm content can reach.
    let DdlBody::Scalar(DomainExpression::Application(FunctionApplication::Case(
        crate::pipeline::asts::core::CaseExpression::Searched { arms, default },
    ))) = &searched.body
    else {
        panic!("the searched form's body is a searched case");
    };
    assert_eq!(arms.len(), 2);
    assert!(default.is_some());
}

/// THE EFFECT ALGEBRA ADMITS PURE AND EFFECT CTEs ALIKE, and the `!` on a
/// binding is an ASSERTION that the body is effectful, never a coercion.
#[test]
fn an_effect_marked_binding_over_a_pure_body_refuses() {
    assert!(file_refusal("main!(*) :-\n    x(*) : q!\n    s!(*)")
        .contains("marked '!' but its body demands no directive"));
}

/// Its lawful twin: an effectful body bound under an effect label and demanded
/// by the final effect call.
#[test]
fn an_effect_marked_binding_over_an_effectful_body_stands() {
    let clause = definition("main!(*) :-\n    x!(*) : q!\n    q!(*)");
    assert_eq!(clause.front.kind, DefKind::Effect);
    assert!(lispy_body(&clause).contains("cte_binding"));
}

/// An edge names a PAIR and baptizes nothing; the pair is held canonically,
/// so the same edge written either way is one subject.
#[test]
fn an_edge_declares_a_pair() {
    let clause = definition("b(*) &(::normal) a(*) :- a(*), b(*)");
    let DefSubject::Edge {
        left,
        right,
        context,
    } = &clause.front.subject
    else {
        panic!("an edge's subject is a pair");
    };
    assert_eq!(left, "a(*)");
    assert_eq!(right, "b(*)");
    assert_eq!(context, "normal");
}

/// A head term SUPPLIES and OFFERS: an unlabeled ground abstains from naming
/// its position, and a label makes it name one.
#[test]
fn a_head_term_supplies_and_offers() {
    use crate::pipeline::asts::core::definitions::Supply;

    let clause = definition("v(x, \"tag\" as kind) :- users(*)");
    let items = clause.front.head.items.listed().expect("a listed head");
    assert!(matches!(items[0].supply, Supply::Ref(_)));
    assert_eq!(
        items[0].offered_name().map(ToString::to_string),
        Some("x".into())
    );
    assert!(matches!(items[1].supply, Supply::Ground(_)));
    assert_eq!(
        items[1].offered_name().map(ToString::to_string),
        Some("kind".into())
    );
}

// ---------------------------------------------------------------------
// Companion cells
// ---------------------------------------------------------------------

/// The COLUMN selects the root, and the category comes from that position —
/// never from the cell's content. `%` is a unique key in a constraint cell
/// and would be modulo anywhere else.
#[test]
fn a_companion_cell_takes_its_category_from_its_column() {
    use crate::ddl_pipeline::asts::{DdlConstraint, DdlDefault};
    use crate::pipeline::normalize::companion;
    use crate::pipeline::syntax::{CompanionColumn, Parser};

    let registry = std::rc::Rc::new(crate::names::Registry::new(&[]));
    let mut parser = Parser::new();

    let primary = parser.parse_companion_cell(CompanionColumn::Constraint, "%%(a, b)");
    assert!(matches!(
        companion::constraint_cell(&primary, registry.clone()).expect("a primary key"),
        DdlConstraint::PrimaryKey { columns: Some(_) }
    ));

    let unique = parser.parse_companion_cell(CompanionColumn::Constraint, "%");
    assert!(matches!(
        companion::constraint_cell(&unique, registry.clone()).expect("a unique key"),
        DdlConstraint::Unique { columns: None }
    ));

    let check = parser.parse_companion_cell(CompanionColumn::Constraint, "@ > 0");
    assert!(matches!(
        companion::constraint_cell(&check, registry.clone()).expect("a check"),
        DdlConstraint::Check { .. }
    ));

    let default = parser.parse_companion_cell(CompanionColumn::Default, "42");
    assert!(matches!(
        companion::default_cell(&default, registry).expect("a default"),
        DdlDefault::Value { .. }
    ));
}

// ---------------------------------------------------------------------
// Call groups
// ---------------------------------------------------------------------

/// A CALL publishes a relation, so a group that SHAPES what it asks for
/// becomes continuations of the chain the call heads. There is no second
/// carrier: the shaping is the ordinary chain's.
#[test]
fn a_shaping_call_group_shapes_the_chain_the_call_heads() {
    for source in [
        "tally(orders(*))(, user_id > 3)",
        "mount!(\"d.sqlite\", \"r\")(*, namespace = \"x\")",
    ] {
        let query = query(source);
        let chain = &query.body;
        assert!(
            matches!(
                chain.head().form(),
                GroundForm::Reference(Relation::FunctorCall { .. })
            ),
            "{source:?} heads with the call"
        );
        assert!(
            chain
                .continuations()
                .iter()
                .any(|c| matches!(c.form(), Continuation::Restrict { .. })),
            "{source:?} keeps its filter as a continuation"
        );
    }
}

/// THE IMPLICIT STAR: an interior continuation starts REALISED, so a call
/// group that shapes without naming is asking for everything.
#[test]
fn a_shaping_call_group_starts_realised() {
    let query = query("tally(orders(*))(, user_id > 3)");
    assert!(shows(&query, "(access (access:all))"));
}

/// An expansion's interior is POSITIONAL: a name binds its position, the
/// anaphor holds one, and a ground term fixes one — and a fixed position
/// still occupies a column slot, so the list stays aligned.
#[test]
fn an_expansion_interior_is_positional() {
    let query = query("a(*) .t(x, \"c\", _)");
    assert!(shows(&query, "(columns [\"x\" \"_\" \"_\"])"));
    assert!(shows(&query, "(groundings [(\"1\" . \"c\")])"));
}

/// THE CATALOG ANSWERS AS DATA: a namespace read is an ordinary relation, so
/// a shaping interior makes it an ordinary derived table.
#[test]
fn a_catalog_read_is_an_ordinary_relation() {
    let plain = query("sys::(*)");
    assert!(shows(&plain, "(access (access:all))"));
    let shaped = query("sys::(|> (entities))");
    let chain = &shaped.body;
    assert!(matches!(
        chain.head().form(),
        GroundForm::Reference(Relation::InnerRelation { .. })
    ));
}

// ---------------------------------------------------------------------
// The sidecars
// ---------------------------------------------------------------------

/// An annotation DECORATES a position: it never changes the relex around it.
/// Each member of the closed set reaches its own collector.
#[test]
fn an_annotation_decorates_and_collects() {
    let plain = query("users(*)");

    let hooked = queries("users(*) (~~error://semantic/arity ~~)");
    assert_eq!(
        hooked
            .queries()
            .nth(0)
            .expect("a goal")
            .declared
            .expected_error
            .as_ref()
            .map(|hook| hook.uri_segments.clone()),
        Some(vec!["semantic".to_string(), "arity".to_string()])
    );
    assert_eq!(
        lispy(&hooked.queries().nth(0).expect("a goal").query),
        lispy(&plain)
    );

    let ddl = queries("(~~ddl v(*) :- users(*) ~~) users(*)");
    assert_eq!(
        ddl.queries()
            .nth(0)
            .expect("a goal")
            .declared
            .ddl_blocks
            .len(),
        1
    );
}

/// A SUBORDINATE BLOCK BELONGS TO ITS FILE. At file scope the block is not a
/// goal and not a definition — it declares a DDL block belonging to the file,
/// and it reaches the file-level carrier every other file-level declaration
/// reaches. There is no second carrier.
#[test]
fn a_file_scope_ddl_block_declares_the_files_own_block() {
    // A file that is nothing but a block: no goal, no definition, one block.
    let alone = file("(~~ddl:\"_internal\"\nschema(\"p\" as entity, name, type) :- _(name, type ---- \"id\", \"INTEGER\")\n~~)");
    assert!(alone.queries().next().is_none());
    assert!(alone.definitions().next().is_none());
    assert_eq!(alone.declared.ddl_blocks.len(), 1);
    // The NAME is the child namespace the block is processed in; the reserved
    // `_internal` suffix reaches it exactly as authored.
    assert_eq!(
        alone.declared.ddl_blocks[0].namespace.as_deref(),
        Some("_internal")
    );
    // The body is TYPED definition content: one clause, already normalized,
    // never a text slice waiting for a consult-time reparse.
    assert_eq!(alone.declared.ddl_blocks[0].body.definitions.len(), 1);
    assert_eq!(
        alone.declared.ddl_blocks[0].body.definitions[0]
            .front
            .name(),
        "schema"
    );
    assert!(alone.declared.ddl_blocks[0].body.ddl_blocks.is_empty());

    // Beside ordinary definitions, on either side of them.
    let beside =
        file("(~~ddl w(*) :- v(*) ~~)\nv(*) :- users(*)\n(~~ddl:\"_internal\" x(*) :- v(*) ~~)");
    assert_eq!(beside.definitions().count(), 1);
    assert_eq!(beside.declared.ddl_blocks.len(), 2);
    // Unnamed is the FILE's own namespace: the suffix is absent, not empty.
    assert_eq!(beside.declared.ddl_blocks[0].namespace, None);
    assert_eq!(
        beside.declared.ddl_blocks[1].namespace.as_deref(),
        Some("_internal")
    );

    // A goal in the same file keeps its own declarations: a file-level block
    // is the FILE's, so no goal claims it.
    let with_goal = file("(~~ddl w(*) :- v(*) ~~)\n?- users(*)");
    assert_eq!(with_goal.queries().count(), 1);
    assert!(with_goal
        .queries()
        .nth(0)
        .expect("a goal")
        .declared
        .ddl_blocks
        .is_empty());
    assert_eq!(with_goal.declared.ddl_blocks.len(), 1);
}

/// A danger gate is a NAMED behavior: the annotation takes the URI ALONE, so
/// there is no state word to read and none to get wrong, and guessing a gate
/// teaches the replacement.
#[test]
fn a_danger_gate_is_acknowledged_by_naming_it() {
    let acknowledged = queries("users(*) (~~danger://cardinality/cartesian ~~)");
    assert_eq!(
        acknowledged
            .queries()
            .nth(0)
            .expect("a goal")
            .declared
            .dangers
            .len(),
        1
    );
    assert!(matches!(
        acknowledged
            .queries()
            .nth(0)
            .expect("a goal")
            .declared
            .dangers[0]
            .state,
        crate::pipeline::asts::core::DangerState::On
    ));
    assert!(refusal("users(*) (~~danger://no/such/gate ~~)").contains("unknown danger gate"));
}

// ---------------------------------------------------------------------
// The named gaps
// ---------------------------------------------------------------------

/// The CLOSED inventory of deferrals: lawful forms the grammar admits and the
/// surviving AST has no carrier for. Every one is reachable, every one names
/// its family, and nothing else exits through this door — the vocabulary is
/// an enum, so a third would have to be declared.
#[test]
fn every_deferred_gap_is_reachable_and_named() {
    use crate::pipeline::normalize::Deferred;

    let reaches: &[(Deferred, &str)] = &[
        (Deferred::DequalifyOrdinal, "users(*.( |1| ))"),
        (Deferred::OperatorOrdinal, "sys::(*) |> .|1|(*)"),
    ];
    // THE RUN IS ONE RUN. The dequalifying step names the same missing
    // carrier inside the parens and after them, so it names ONE family —
    // two would let the same gap be reported as two.
    assert!(refusal("users(*) .( |1| )").contains(Deferred::DequalifyOrdinal.family()));
    for (family, source) in reaches {
        let refusal = refusal(source);
        assert!(
            refusal.contains(family.family()) && refusal.contains("no carrier yet"),
            "{source:?} should defer as the {} gap; got: {refusal}",
            family.family()
        );
    }

    // Every declared deferral is exercised above: the inventory is the enum,
    // and this is the enumeration that proves the list is not stale. What
    // remains is the two ORDINAL families; the mode and the pick are built.
    assert_eq!(Deferred::ALL.len(), reaches.len());
    assert!(Deferred::ALL
        .iter()
        .all(|d| d.family() == "dequalify" || d.family() == "operator column"));
}

/// ONE CARRIER, EVERY NONEMPTY WIDTH. A one-input/one-output mode is not an
/// anchored case that happens to be one row — it is the same declaration, in
/// the same shape, read by the same code as the wider form.
#[test]
fn one_fact_function_carrier_serves_every_width() {
    use crate::pipeline::asts::core::FactFunctionMode;

    fn mode(source: &str) -> FactFunctionMode<Unresolved> {
        let clause = definition(source);
        assert_eq!(clause.front.kind, DefKind::FactFunction);
        let DdlBody::FactFunction(definition) = clause.body else {
            panic!("a fact function's body is its declared mode");
        };
        definition.mode().clone()
    }

    let narrow = mode("style_of(v -> s ---- \"a\" -> \"x\"; _ -> \"y\")");
    assert_eq!(narrow.inputs.len(), 1);
    assert_eq!(narrow.outputs.len(), 1);
    assert_eq!(narrow.arms.len(), 1);
    assert_eq!(narrow.arms.first().inputs.len(), 1);
    assert!(narrow.default.is_some());

    let wide = mode("f(a, b -> c, d ---- 1, 2 -> 3, 4; 5, 6 -> 7, 8)");
    assert_eq!(wide.inputs.len(), 2);
    assert_eq!(wide.outputs.len(), 2);
    assert_eq!(wide.arms.len(), 2);
    assert_eq!(wide.arms.first().inputs.len(), 2);
    assert_eq!(wide.arms.first().outputs.len(), 2);
    assert!(wide.default.is_none());

    // THE SEPARATOR IS ONE PRODUCTION: `@` and the dashes are two spellings
    // of it, and the carrier cannot tell them apart.
    assert_eq!(mode("f(a, b -> c, d @ 1, 2 -> 3, 4; 5, 6 -> 7, 8)"), wide);

    // The declared heading is callable metadata for both modes. Only the
    // finite mode can mint a relational body; a default is an unbounded
    // complement, not a row that can be omitted from an approximation.
    assert_eq!(
        narrow.heading().len(),
        2,
        "the heading is the inputs then the outputs"
    );
    let narrow_group =
        crate::ddl::reconstruct::group("style_of(v -> s ---- \"a\" -> \"x\"; _ -> \"y\")")
            .expect("the complete definition assembles");
    assert!(!narrow_group.entity_type().realizes_relation());
    let wide_group =
        crate::ddl::reconstruct::group("f(a, b -> c, d ---- 1, 2 -> 3, 4; 5, 6 -> 7, 8)")
            .expect("the complete finite definition assembles");
    let wide_body = wide_group
        .spend_heads()
        .expect("the finite face token spends")
        .into_iter()
        .next()
        .and_then(crate::pipeline::asts::ddl::Clause::into_query)
        .expect("a finite fact function has a relational body");
    let Ok(chain) = wide_body.into_bare_body() else {
        panic!("the elaborated face is a flat relation");
    };
    let lispy = crate::lispy::ToLispy::to_lispy(&chain);
    assert_eq!(
        lispy.matches("tabular_row").count(),
        3,
        "one header row and two finite arm rows: {lispy}"
    );
}

/// A DECLARED WIDTH IS A DECLARED WIDTH, and the refusal is the definition's.
#[test]
fn a_fact_function_row_that_disagrees_with_its_head_refuses() {
    for (source, position) in [
        ("f(a, b -> c ---- 1 -> \"x\")", "arm's match row"),
        ("f(a -> c, d ---- 1 -> \"x\")", "arm's output row"),
        (
            "f(a -> c, d ---- 1 -> \"x\", \"y\"; _ -> \"z\")",
            "default output row",
        ),
    ] {
        let refused = definition_refusal(source);
        assert!(
            refused.contains(position) && refused.contains("declares"),
            "{source:?} should refuse at the {position}; got: {refused}"
        );
    }
}

/// THE DECLARED INPUTS ARE THE OUTPUT CELLS' BINDERS, and the only ones.
/// There is no enclosing row in either face — relationally these cells are a
/// fact's data, callably the argument row is all there is — so any other
/// name addresses nothing and a qualifier addresses nothing at all.
#[test]
fn a_fact_function_output_cell_reads_only_its_declared_inputs() {
    // A declared input binds, and the carrier keeps the reference for the
    // two faces to spend their own way.
    let clause = definition("f(a -> c ---- 1 -> a + 1)");
    let DdlBody::FactFunction(definition) = &clause.body else {
        panic!("a fact function's body is its declared mode");
    };
    let mode = definition.mode();
    assert!(mode.default.is_none());

    // THE RELATIONAL FACE SPENDS THE ARM'S OWN MATCH ROW, so the published
    // row is ground — a fact's row, with no reference left in it.
    let finite = crate::ddl::reconstruct::group("f(a -> c ---- 1 -> a + 1)")
        .expect("the complete finite definition assembles");
    let query = finite
        .spend_heads()
        .expect("the finite face token spends")
        .into_iter()
        .next()
        .and_then(crate::pipeline::asts::ddl::Clause::into_query)
        .expect("a finite fact function has a relational body");
    let lispy = crate::lispy::ToLispy::to_lispy(&query);
    assert!(
        !lispy.contains("reference"),
        "the elaborated face is ground: {lispy}"
    );

    for (source, reported) in [
        ("f(a -> c ---- 1 -> b)", "'b'"),
        ("f(a -> c ---- 1 -> t.a)", "'t.a'"),
        // A strop is spelling: the unstropped spelling is another name.
        ("f(`In Put` -> c ---- 1 -> in_put)", "'in_put'"),
        // The default is a cell like any other.
        ("f(a -> c ---- 1 -> a; _ -> b)", "'b'"),
    ] {
        let refused = definition_refusal(source);
        assert!(
            refused.contains("is not one of its declared inputs") && refused.contains(reported),
            "{source:?} should refuse naming {reported}; got: {refused}"
        );
    }
}

/// A DECLARED NAME IS DECLARED ONCE, over the WHOLE declared heading. A
/// repeated output leaves a pick with two winners; a repeated input leaves a
/// cell with two binders; and either way the relational heading, which is
/// the inputs followed by the outputs, holds one name at two positions.
#[test]
fn a_fact_function_declares_each_name_once() {
    for (source, collision) in [
        ("f(a -> b, b ---- 1 -> 2, 3)", "'b' twice as an output"),
        ("f(a, a -> c ---- 1, 2 -> 3)", "'a' twice as an input"),
        // THE TWO LISTS ARE ONE HEADING: a name declared on both sides
        // publishes twice relationally, so it is the same collision.
        (
            "f(a -> a ---- 1 -> a)",
            "'a' once as an input and once as an output",
        ),
    ] {
        let refused = definition_refusal(source);
        assert!(
            refused.contains(collision),
            "{source:?} should refuse with {collision}; got: {refused}"
        );
    }
    // Two spellings that differ are two names, so this one stands.
    let clause = definition("f(a -> b, `B` ---- 1 -> 2, 3)");
    let DdlBody::FactFunction(definition) = &clause.body else {
        panic!("a fact function's body is its declared mode");
    };
    let mode = definition.mode();
    assert_eq!(mode.outputs.len(), 2);
}

/// `foo:(x).out1` builds the PICK, and the declaration it picks from is the
/// callee's — so the authored phase carries no proof of one.
#[test]
fn a_field_select_builds_its_exact_carrier() {
    let query = super::support::query("users(*) |> (foo:(x).out1 as picked)");
    let lispy = super::support::lispy(&query);
    assert!(
        lispy.contains("field_select"),
        "the pick has its own carrier: {lispy}"
    );
    assert!(
        !lispy.contains("no carrier yet"),
        "and it is not a gap: {lispy}"
    );
}

/// A sidecar belongs to the QUERY that declared it. In a sequence, the
/// submission cannot say which query acknowledged a danger — only the goal
/// can — so nothing may bleed forward or backward.
#[test]
fn a_sequence_keeps_each_query_s_sidecars_with_that_query() {
    let declaring = "users(*) (~~danger://cardinality/cartesian ~~)";
    let plain = "orders(*)";

    for (source, declared_at) in [
        (format!("{declaring}\n{plain}"), 0usize),
        (format!("{plain}\n{declaring}"), 1usize),
    ] {
        let normalized = queries(&source);
        assert_eq!(normalized.queries().count(), 2, "{source:?}");
        let quiet = 1 - declared_at;

        assert_eq!(
            normalized
                .queries()
                .nth(declared_at)
                .expect("a goal")
                .declared
                .dangers
                .len(),
            1,
            "the declaring query keeps its danger in {source:?}"
        );
        assert!(
            normalized
                .queries()
                .nth(quiet)
                .expect("a goal")
                .declared
                .dangers
                .is_empty(),
            "the quiet query declares nothing in {source:?}"
        );
        assert!(
            normalized.declared.dangers.is_empty(),
            "a goal's declarations do not also stand on the submission"
        );
    }
}

/// A definition's declarations are the FILE's — a definition is itself a
/// file-level form — and they do not reach a goal that follows it.
#[test]
fn a_definition_s_declarations_do_not_reach_the_next_goal() {
    let normalized = file("v(*) :- users(*) (~~danger://cardinality/cartesian ~~)\n?- orders(*)");
    assert_eq!(normalized.definitions().count(), 1);
    assert_eq!(normalized.queries().count(), 1);
    assert_eq!(normalized.declared.dangers.len(), 1);
    assert!(normalized
        .queries()
        .nth(0)
        .expect("a goal")
        .declared
        .dangers
        .is_empty());
}

// ---------------------------------------------------------------------
// Higher-order bindings
// ---------------------------------------------------------------------

/// The invocation entrance: the SAME source, normalized again with the call
/// site's bindings in hand. Every family the old invocation road supports
/// reaches the resulting AST here.
mod bindings {
    use super::super::support::*;
    use crate::pipeline::asts::core::*;
    use crate::pipeline::asts::ddl::DdlBody;
    use crate::pipeline::normalize;
    use crate::pipeline::query_features::HoParamBindings;
    use crate::pipeline::syntax::Parser;
    use std::rc::Rc;

    fn bound(source: &str, bindings: HoParamBindings) -> Query<Unresolved> {
        let tree = Parser::new().parse_query_sequence(source);
        assert!(!tree.has_defects(), "the grammar refused {source:?}");
        let normalized = normalize::bound_query_sequence(
            &tree,
            Rc::new(crate::names::Registry::new(&[])),
            bindings,
        )
        .unwrap_or_else(|error| panic!("normalizing {source:?} failed: {error}"));
        assert_eq!(normalized.queries().count(), 1);
        normalized.into_queries().remove(0).query
    }

    fn head(query: &Query<Unresolved>) -> &Grelex<Unresolved> {
        &query.body.head()
    }

    fn read_access(query: &Query<Unresolved>) -> &Access<Unresolved> {
        query
            .body
            .head_access()
            .expect("the read carries its access")
    }

    /// A formal bound to a compiler-owned CARRIER is read by IDENTITY: no
    /// character-bearing lookup key participates. This is the road an
    /// interior CTE the invocation materialized reaches the body by, and the
    /// carrier's declared columns become the caller pattern.
    #[test]
    fn a_table_scope_parameter_becomes_a_plan_read() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let scope = registry.authority().reserve_proffer();
        let mut bindings = HoParamBindings::default();
        bindings.table_scope_params.insert("V".to_string(), scope);
        bindings
            .argumentative_patterns
            .insert("V".to_string(), vec!["id".to_string(), "total".to_string()]);
        let query = bound("V(*)", bindings);
        let GroundForm::Reference(Relation::Ground { mention, .. }) = head(&query).form() else {
            panic!("expected a ground read");
        };
        let access = read_access(&query);
        assert!(
            matches!(mention, GroundMention::Structural { pending: bound, .. } if *bound == scope),
            "the carrier is addressed by identity"
        );
        // A glob access over a declared carrier substitutes the DECLARATION's
        // names: argumentative binding is positional, and the supplied
        // relation's own spellings never reach the body.
        let Access::Slots(slots) = access else {
            panic!("expected the declared caller pattern, got {access:?}");
        };
        assert_eq!(slots.len(), 2);
    }

    /// A formal bound to a relation EXPRESSION arrives whole.
    #[test]
    fn a_table_expression_parameter_arrives_whole() {
        let mut bindings = HoParamBindings::default();
        bindings.table_expr_params.insert(
            "V".to_string(),
            Chain::authored(GroundForm::Literal(AnonRelation::plain(
                AnonTable::from_values(
                    None,
                    vec![vec![DomainExpression::Application(
                        FunctionApplication::Ground(LiteralValue::Number("1".into())),
                    )]],
                )
                .unwrap(),
            ))),
        );
        let query = bound("V(*)", bindings);
        assert!(matches!(head(&query).form(), GroundForm::Literal(_)));
    }

    /// A scalar formal RIDES AS A REFERENCE in value position — the body's
    /// formal frame answers it at resolution — while the compile-time
    /// whole-number positions (a row bound, an ordinal) read the literal
    /// binding, because their value must exist before resolution.
    #[test]
    fn a_scalar_parameter_reaches_value_and_bound_positions() {
        let mut bindings = HoParamBindings::default();
        bindings.scalar_formals.insert("n".to_string());
        bindings
            .scalar_literals
            .insert("n".to_string(), LiteralValue::Number("7".into()));

        let valued = bound("users(*) |> (n)", bindings.clone());
        assert!(shows(&valued, "(name \"n\")"), "got: {}", lispy(&valued));

        let bounded = bound("users(*), # < n", bindings.clone());
        assert!(shows(&bounded, "(value 7)"));

        let ordinal = bound("users(*) |> (|n|)", bindings);
        assert!(shows(&ordinal, "reference:ordinal"));
        assert!(shows(&ordinal, "|7|"));
    }

    /// A qualified name addresses somebody else's column: it is never a
    /// formal, so a binding of the same spelling leaves it alone.
    #[test]
    fn a_qualified_name_is_never_a_scalar_formal() {
        let mut bindings = HoParamBindings::default();
        bindings.scalar_formals.insert("n".to_string());
        let query = bound("users(*) |> (t.n)", bindings);
        assert!(shows(&query, "(name \"n\")"));
        assert!(shows(&query, "(qualifier \"t\")"));
    }

    /// No fabricated stand-in. A parameterized body whose bound names a
    /// formal is DEFERRED — the authored characters, held as such — and the
    /// same body normalizes for real once the call site supplies the value.
    #[test]
    fn an_unsubstituted_bound_defers_the_body_instead_of_inventing_one() {
        let clause = definition("top_n(T(*), n)(*) :- T(*), # < n");
        let DdlBody::Deferred { source } = &clause.body else {
            panic!("expected a deferred body, got {:?}", clause.body);
        };
        assert!(source.contains("# < n"));

        // The same source, with the binding in hand, is not deferred at all.
        let mut bindings = HoParamBindings::default();
        bindings.scalar_formals.insert("n".to_string());
        bindings
            .scalar_literals
            .insert("n".to_string(), LiteralValue::Number("3".into()));
        assert!(shows(&bound("users(*), # < n", bindings), "(value 3)"));
    }

    /// An argumentative param bound BY NAME names its relation through the
    /// arity-checked entry, and a qualifier of that formal substitutes
    /// from it.
    #[test]
    fn an_argumentative_by_name_qualifier_becomes_the_supplied_name() {
        let mut bindings = HoParamBindings::default();
        bindings.argumentative_table_refs.push((
            "W".to_string(),
            delightql_types::SqlIdentifier::new("refs"),
            1,
            vec!["key".to_string()],
        ));
        let query = bound("users(*) |> (W.key)", bindings);
        assert!(
            shows(&query, "(qualifier \"refs\")"),
            "the arity-checked binding did not substitute\n  got: {}",
            lispy(&query)
        );
    }

    /// A compiler-owned carrier is addressed by IDENTITY and its plan read
    /// carries the AUTHORED formal: no table spelling exists for a
    /// qualifier to convert to.
    #[test]
    fn a_carrier_qualifier_keeps_the_authored_formal() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let scope = registry.authority().reserve_proffer();
        let mut bindings = HoParamBindings::default();
        bindings.table_scope_params.insert("V".to_string(), scope);
        let query = bound("V(*) |> (V.key)", bindings);
        assert!(
            shows(&query, "(qualifier \"V\")"),
            "the carrier's authored formal was converted\n  got: {}",
            lispy(&query)
        );
    }
}

// ---------------------------------------------------------------------
// The membership probe and the whole-heading correlation
// ---------------------------------------------------------------------

/// THE ANON HEADER IS A SLOT ROW, and an existence-marked one IS the inverted
/// membership: `+_("MA" @ bst; dst)` says what `"MA" in (bst; dst)` says. The
/// evidence rides ON the table, decided where the marker was written.
#[test]
fn an_existence_marked_anon_table_is_the_inverted_membership() {
    for (source, negated) in [
        ("users(*), +_(status @ \"a\"; \"b\")", false),
        ("users(*), \\+_(status @ \"a\")", true),
    ] {
        let query = query(source);
        let chain = &query.body;
        let Some(Continuation::Restrict {
            condition: TruthExpression::Membership(membership),
            ..
        }) = chain.continuations().last().map(|step| step.form())
        else {
            panic!("{source:?} is a membership restriction");
        };
        assert_eq!(
            membership.negated, negated,
            "{source:?} carries its polarity"
        );
    }

    // The MELT reading is the unmarked one, and the two stay apart.
    let melted = query("users(*), _(status @ \"a\")");
    let chain = &melted.body;
    let Some(Continuation::Member { rhs, .. }) =
        chain.continuations().last().map(|step| step.form())
    else {
        panic!("expected a member");
    };
    let GroundForm::Literal(_) = rhs.head().form() else {
        panic!("expected an anonymous table");
    };
}

/// THE WHOLE HEADING CORRELATES, in the mode the step aligns by. The two
/// modes are two forms because the columns they pair are found two
/// different ways, and neither is a truth: the comma member that can hold
/// one is its own continuation.
#[test]
fn a_whole_heading_correlation_carries_its_mode() {
    let by_name = query("a(*) ; b(*), x.* = y.*");
    assert!(shows(&by_name, "continuation:correlate"));
    assert!(shows(&by_name, "whole_heading:by_name"));

    let by_position = query("a(*) || b(*), first|*| = second|*|");
    assert!(shows(&by_position, "continuation:correlate"));
    assert!(shows(&by_position, "whole_heading:by_position"));
    assert!(shows(&by_position, "(left \"first\")"));
    assert!(shows(&by_position, "(right \"second\")"));

    // `and` at the top of a comma member means what two comma members mean,
    // so a correlation conjoined with a predicate is BOTH continuations.
    let conjoined = query("a(*) ; b(*), x.* = y.* and a.k > 1");
    assert!(shows(&conjoined, "continuation:correlate"));
    assert!(shows(&conjoined, "continuation:restrict"));

    // A correlation is not a truth, so no truth position admits one.
    assert!(refusal("a(*) ; b(*), !(x.* = y.*)").contains("does not stand where a truth is read"));
    assert!(refusal("a(*) ; b(*), x.* = y.* or a.k > 1")
        .contains("does not stand where a truth is read"));

    // The modes never mix, a correlation is written with `=`, and both
    // operands name the arm they address.
    assert!(refusal("a(*) || b(*), x.* = y|*|").contains("NAME or by POSITION"));
    assert!(refusal("a(*) || b(*), x|*| != y|*|").contains("written with '='"));
    assert!(refusal("a(*) || b(*), * = y.*").contains("names none"));
}

/// `ho_part as on every functor`: an inner form NAMES a relation, and a
/// relation the caller parameterizes is the same relation. Three positions
/// take the argument row, and all three reach the one call carrier.
#[test]
fn every_functor_position_takes_its_argument_row() {
    // An existence probe.
    let probe = query("users(*), +f(a(*))(, x = 1)");
    assert!(shows(&probe, "truth_expression:existence"));
    assert!(shows(&probe, "ho_argument:relation"));

    // An inner form in value position.
    let inner = query("users(*) |> (f:(a(*))(, x = 1 ~> count:(*)))");
    assert!(shows(&inner, "domain_expression:scalar_subquery"));
    assert!(shows(&inner, "ho_argument:relation"));

    // An outer access: `?` is written on the ACCESS, and a higher-order
    // access is an access.
    let outer = query("f?(a(*))(*), b(*)");
    assert!(shows(&outer, "relation:functor_call"));
    assert!(shows(&outer, "FunctorMarks { outer: true"));
}

/// THE LIFT'S ROWS ARE ONE RELATION. `&` BOUNDS the ordinary arguments, so
/// with no `&` there is nothing to bound and every `;`-separated row belongs
/// to the lift. Splitting one written row-set across two roles passes a
/// one-row relation where the author wrote several and drops the rest with no
/// diagnostic.
#[test]
fn an_unbounded_argument_row_set_lifts_whole() {
    // No `&`: the rows are the argument, and nothing stands beside them.
    let unbounded = query("f(\"a\"; \"b\")(*)");
    assert!(shows(&unbounded, "ho_argument:relation"));
    assert!(shows(&unbounded, "(literal_value:string \"a\")"));
    assert!(shows(&unbounded, "(literal_value:string \"b\")"));
    // One argument, not two: a scalar argument beside the lift would be a
    // second `ho_argument`.
    assert_eq!(lispy(&unbounded).matches("ho_argument:").count(), 1);

    // `&` bounds: the relation left of it is an argument, the rows right of
    // it are the lift.
    let bounded = query("f(users(*) & 1, 2; 10, 20)(*)");
    assert_eq!(lispy(&bounded).matches("ho_argument:").count(), 2);

    // Multi-column rows without a bound stay one relation of two rows. The
    // rows are the shared tabular geometry's, so they are counted under the
    // name that carrier actually publishes.
    let wide = query("f(1, 2; 10, 20)(*)");
    assert_eq!(lispy(&wide).matches("ho_argument:").count(), 1);
    assert_eq!(lispy(&wide).matches("(tabular_row ").count(), 2);

    // A comma list with no `;` is an argument list, not a one-row relation.
    let arguments = query("f(1, 2)(*)");
    assert_eq!(lispy(&arguments).matches("ho_argument:").count(), 2);
}

/// An expansion's parens NAME the interior heading; they do not shape or
/// compute one. Each way of trying refuses under its own identity rather than
/// arriving as a generic gap.
#[test]
fn an_expansion_interior_names_and_does_not_compute() {
    assert!(refusal("a(*) .t(|> (x))").contains("names the interior columns"));
    assert!(refusal("a(*) .t(x, upper:(y))").contains("fixes them to constants"));
    assert!(refusal("a(*) .t(x, (y = 1))").contains("fixes them to constants"));
    // The lawful shapes still build.
    assert!(shows(&query("a(*) .t(*)"), "(glob true)"));
    assert!(shows(
        &query("a(*) .t(x, \"c\")"),
        "(groundings [(\"1\" . \"c\")])"
    ));
}

/// A narrowing DECLARES A TREE PATTERN — the same members a `~=` destructure
/// declares, built by the same road — and publishes its fields and NOTHING
/// else, so a member with no output to attach to refuses rather than being
/// dropped.
#[test]
fn a_narrowing_declares_a_tree_pattern() {
    assert!(shows(&query("a(*) |> .t{.a.b}"), "pattern_member:path"));
    assert!(shows(&query("a(*) |> .t{n}"), "pattern_member:binder"));
    assert!(refusal("a(*) |> .t{\"k\": n}").contains("names the fields its payload publishes"));
}

/// ONE WIDTH JUDGMENT FOR EVERY TABULAR INTERIOR. A row that does not fit its
/// heading has cells belonging to no column, and the refusal is the same one
/// whichever body wrote it — an anonymous table or a fact.
#[test]
fn one_algorithm_judges_every_tabular_width() {
    assert!(refusal("_(a, b @ 1, 2; 3)").contains("carries 1 cell(s)"));
    assert!(definition_refusal("t(a, b ---- 1, 2; 3)").contains("carries 1 cell(s)"));

    // With no written heading the FIRST ROW carries the width, on both roads.
    assert!(refusal("_(1, 2; 3)").contains("its first row"));
}

/// THE CELL ADMISSIONS STAY DISTINCT. Sharing the geometry shares the width
/// and sparse judgments, not what may stand in a cell: an anonymous datum is
/// a domain expression, and a fact datum is ground.
#[test]
fn the_two_bodies_admit_different_data() {
    // An anonymous datum computes: a call stands in one.
    assert!(shows(
        &query("_(a @ upper:(\"x\"))"),
        "domain_expression:functor_call"
    ));
    // A fact datum does not: the grammar admits `ground` alone, so the same
    // spelling has no derivation on that side.
    assert!(definition_refusal("t(a ---- upper:(\"x\"))").contains("Parse error"));
}

/// THE SOURCELESS INNER FORM has no OUTER base — the body resolves against
/// the enclosing row — but its interior supplies one of its own, and a
/// compression with nothing to reduce refuses.
#[test]
fn a_sourceless_inner_form_supplies_its_own_base() {
    assert!(shows(
        &query("users(*) |> (_:(, _(1; 2) ~> count:(*)))"),
        "domain_expression:scalar_subquery"
    ));
    assert!(refusal("users(*) |> (_:(, x > 1 ~> count:(*)))")
        .contains("supplies its own base relation"));
}

/// A STRUCTURAL FORM PUBLISHES A STAGE, and `as` names it: the name lands
/// on the step's own slot — the same slot a pipe operator's stage name
/// takes — never dropped, never a refusal. A restriction publishes no
/// stage and still refuses.
#[test]
fn a_structural_form_takes_a_stage_name() {
    let chain = query("users(*) |> #(age) as s").body;
    let Some(Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
        form: crate::pipeline::asts::core::StructuralForm::Ordering { .. },
        named,
        ..
    })) = chain.continuations().last().map(|step| step.form())
    else {
        panic!(
            "expected a trailing ordering, got {:?}",
            chain.continuations().last()
        );
    };
    assert_eq!(named.as_ref().map(|n| n.as_str()), Some("s"));
    assert!(refusal("users(*), age > 1 as z").contains("publishes no pipe stage"));
}

// ---------------------------------------------------------------------
// Consumed or refused (R4.2.8): authored syntax that used to parse and
// silently disappear now refuses with a named teaching.
// ---------------------------------------------------------------------

/// The badge a query-mode binding carries out of normalization.
fn binding_badge(source: &str) -> Fixpoint {
    let normalized = queries(source);
    let goal = normalized.into_queries().remove(0);
    goal.query.ctes()[0].authority().fixpoint
}

/// THE BADGE CHOOSES THE UNION, and normalization CARRIES the choice under
/// both surfaces rather than acting on it: whether the subject is a fixpoint
/// at all is not knowable here, so the flavor rides the clause and the
/// binding to the one recursion decision.
#[test]
fn a_fixpoint_badge_is_carried_from_both_surfaces() {
    assert_eq!(
        file("cnt%(*) :- _(n @ 1)")
            .definitions()
            .nth(0)
            .expect("a definition")
            .front
            .fixpoint,
        Fixpoint::Deduplicating
    );
    assert_eq!(
        file("cnt(*) :- _(n @ 1)")
            .definitions()
            .nth(0)
            .expect("a definition")
            .front
            .fixpoint,
        Fixpoint::Bag
    );
    assert_eq!(
        binding_badge("c%(*) : _(n @ 1)\nc(*)"),
        Fixpoint::Deduplicating
    );
    assert_eq!(binding_badge("c(*) : _(n @ 1)\nc(*)"), Fixpoint::Bag);
    // The LABEL shorthand badges the same way: `body : c%` is `c%(*) : body`.
    assert_eq!(
        binding_badge("_(n @ 1) : c%\nc(*)"),
        Fixpoint::Deduplicating
    );
    assert_eq!(binding_badge("_(n @ 1) : c\nc(*)"), Fixpoint::Bag);
}

/// A signature declares its capture once: a second context marker would
/// silently replace the first, so it refuses.
#[test]
fn a_second_context_marker_refuses() {
    let tree =
        crate::pipeline::syntax::Parser::new().parse_definition_file("f:(.., x, ..{a}) :- x + 1");
    assert!(!tree.has_defects());
    let registry = std::rc::Rc::new(crate::names::Registry::new(&[]));
    let err = crate::pipeline::normalize::definition_file(&tree, registry)
        .expect_err("two captures have no reading");
    assert_eq!(
        err.error_uri(),
        "delightql-error://semantic/ddl/head/duplicate_context_marker"
    );
    // One marker stays lawful.
    assert!(!file("f:(.., x) :- x + 1").definitions().next().is_none());
}

/// A parameterized fact's header names its output positions: a slot that
/// names nothing would be silently dropped from the declared head while the
/// table keeps its width, so it refuses.
#[test]
fn a_nameless_parameterized_fact_header_item_refuses() {
    let tree = crate::pipeline::syntax::Parser::new()
        .parse_definition_file("f(T(*))(a, _, c ---- 1, 2, 3)");
    assert!(!tree.has_defects());
    let registry = std::rc::Rc::new(crate::names::Registry::new(&[]));
    let err = crate::pipeline::normalize::definition_file(&tree, registry)
        .expect_err("a nameless header item names no output position");
    assert_eq!(
        err.error_uri(),
        "delightql-error://semantic/ddl/head/fact_header"
    );
}
