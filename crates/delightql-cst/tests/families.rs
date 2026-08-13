// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! Conformance across the semantic families.
//!
//! Each test names a distinction SEMANTICS/ draws and shows the grammar drawing
//! it. "Parses without error" is not the bar: two forms that must mean
//! different things have to produce different typed shapes, and two spellings
//! of one thing have to produce the same one.

mod support;

use delightql_cst::cst::*;
use delightql_cst::{CompanionColumn, Parser, Root, TypedNode};
use support::{admits, admits_file, count, first, refuses_file, text_of, refuses_query};

// ---------------------------------------------------------------------------
// The relex line
// ---------------------------------------------------------------------------

/// The four named functor shapes are told apart by their interior alone, and
/// the interior is not classified by content — an empty paren, a slot row and a
/// continuation chain are three different productions.
#[test]
fn the_functor_family_splits_by_interior() {
    let inchoate = admits("users()");
    assert_eq!(count::<InchoateFunctor>(&inchoate), 1);
    assert_eq!(count::<ArgumentativeFunctor>(&inchoate), 0);

    let argumentative = admits("users(id, name)");
    assert_eq!(count::<ArgumentativeFunctor>(&argumentative), 1);
    assert_eq!(count::<Slot>(&argumentative), 2);

    let interior = admits("users(, age > 3)");
    assert_eq!(count::<InteriorFunctor>(&interior), 1);
    assert_eq!(count::<CommaContinuation>(&interior), 1);

    let catalog = admits("sys::entities::(*)");
    assert_eq!(count::<CatalogFunctor>(&catalog), 1);
}

/// THE IMPLICIT STAR: an interior continuation always starts realised, so the
/// `*` in `users(*)` is the qualify postfix and `p(C) ≡ p(*) C`. There is no
/// second glob carrier competing for it.
#[test]
fn the_star_in_an_interior_is_the_qualify_postfix() {
    let tree = admits("users(*)");
    assert_eq!(count::<DomainActivate>(&tree), 1);
    assert_eq!(count::<Glob>(&tree), 0, "an interior star is not a glob");

    // The head positions keep the literal glob: no continuation is admitted
    // there, so nothing competes.
    let head = admits_file("adults(*) :- users(*)");
    assert_eq!(count::<GlobHeading>(&head), 1);
    assert_eq!(count::<Glob>(&head), 1);
}

/// THE ENGINE'S CATALOG IS THE ENGINE'S: the dot is DQL's catalog, the slash is
/// the engine's, and the two never reach the same carrier.
#[test]
fn the_slash_and_the_dot_are_different_names() {
    let engine = admits("main/sqlite_master(*)");
    assert_eq!(text_of::<EngineReference>(&engine), "main/sqlite_master");

    let catalog = admits("lib::math.helper(*)");
    assert_eq!(count::<EngineReference>(&catalog), 0);
    assert_eq!(text_of::<NamespaceQual>(&catalog), "lib::math.");
}

/// THE SLASH RIDES THE NAME, and a number after it is DIVISION. An engine
/// reference is `identifier '/' identifier`, so `x/2.2` can only be
/// arithmetic — which it is exactly because the reference matches the whole
/// `/name`. Split into a slash of its own, the immediate token would win
/// before any name was seen, and no spacing-free division would parse at all.
#[test]
fn a_number_after_the_slash_is_division() {
    for src in ["users(*) |> (x/2.2)", "users(*) |> (x/2)"] {
        admits(src);
    }
    assert_eq!(count::<EngineReference>(&admits("users(*) |> (x/2.2)")), 0);

    // A NAME after the slash still commits to the reference, wherever it
    // stands: `x/y` in value position refuses, though only division derives
    // there. That half needs the parser's position, which no lexer has.
    refuses_query("users(*) |> (x/y)");

    // …while a name after it is still the reference, strop included.
    let stropped = admits("main/`Mixed Name`(*)");
    assert_eq!(count::<EngineReference>(&stropped), 1);
    assert_eq!(text_of::<EngineName>(&stropped), "/`Mixed Name`");
    assert_eq!(text_of::<EngineName>(&admits("main/orders(*)")), "/orders");
}

/// An engine reference heads a PURE RELATION ACCESS wherever there is one.
/// Every relational position spells the same `relation_name`, so admitting the
/// slash in one of them and not another could only be an oversight — and the
/// oversight bites silently: `main/orders:(…)` read as `main` DIVIDED by a
/// scalar subquery, which resolution reports as a missing column named `main`.
#[test]
fn every_relational_position_takes_the_engine_reference() {
    for src in [
        // the functor family
        "main/users(*)",
        "main/users(, id = 1)",
        // outer-marked
        "users(*), main/orders?(*)",
        // existence, in truth and in value position
        "users(*), +main/orders(, user_id = users.id)",
        "users(*) |> (+main/orders(*) as has_order)",
        // relational membership
        "users(*), id in main/orders(*)",
        // the inner form
        "users(*) |> (main/orders:(~> count:(*)) as n)",
        // the piped invocation
        "users(*) |> main/orders(*)",
    ] {
        let tree = admits(src);
        assert_eq!(count::<EngineReference>(&tree), 1, "{src}");
    }
}

/// A CALLABLE's name is not a relation's. The slash names no function, so the
/// narrow spelling stays where it belongs.
#[test]
fn a_callable_never_takes_the_engine_reference() {
    support::refuses_query("users(*) |> (main/upper:(first_name) as x)");
    support::refuses_query("users(*), +main/big(age)");
}

/// RETENTION DECIDES POSITION: the context-keeping form is postfix, the
/// payload-only form is post-pipe. Position classifies; no descriptor or
/// content ever does.
#[test]
fn drill_and_narrow_are_told_apart_by_position() {
    let drill = admits("users(*).t(*)");
    assert_eq!(count::<Drill>(&drill), 1);
    assert_eq!(count::<NarrowingAccess>(&drill), 0);

    let narrow = admits("users(*) |> .t(*)");
    assert_eq!(count::<NarrowingAccess>(&narrow), 1);
    assert_eq!(count::<Drill>(&narrow), 0);
}

/// A bound has ONE home — the comma member.
#[test]
fn a_bound_is_a_comma_member() {
    let tree = admits("users(*), #<10");
    let bound = first::<RowBound>(&tree);
    assert_eq!(tree.text(bound), "#<10");
    // The lexical layer has always allowed the space; one carrier, two
    // spellings.
    assert_eq!(text_of::<RowBound>(&admits("users(*), # < 10")), "# < 10");
}

/// The set operators are four distinct continuations, not one with a payload.
#[test]
fn each_set_operator_is_its_own_continuation() {
    assert_eq!(
        count::<PositionalUnionContinuation>(&admits("a(*) || b(*)")),
        1
    );
    assert_eq!(count::<SmartUnionContinuation>(&admits("a(*) |;| b(*)")), 1);
    assert_eq!(
        count::<CorrespondingUnionContinuation>(&admits("a(*) ; b(*)")),
        1
    );
    assert_eq!(count::<MinusContinuation>(&admits("a(*) - b(*)")), 1);
}

/// '&' selects declared edges, '&&' walks them; the context rides on the
/// operator as a light mention.
#[test]
fn the_edge_operators_carry_their_context() {
    let tree = admits("users_t(*) &(::normal) orders_t(*)");
    let edge = first::<EdgeContinuation>(&tree);
    assert_eq!(edge.operator().map(|o| tree.text(o)), Some("&"));
    assert_eq!(edge.context().map(|c| tree.text(c)), Some("(::normal)"));

    let walk = admits("users_t(*) && orders_t(*)");
    assert_eq!(
        first::<EdgeContinuation>(&walk)
            .operator()
            .map(|o| walk.text(o)),
        Some("&&")
    );
}

/// `^^` is two adjacent applications, never a token: the tower is built by
/// ordinary postfix stacking.
#[test]
fn meta_stacks_rather_than_spelling_a_second_token() {
    assert_eq!(count::<Meta>(&admits("users(*) ^")), 1);
    assert_eq!(count::<Meta>(&admits("users(*) ^^")), 2);
}

// ---------------------------------------------------------------------------
// Value and truth position
// ---------------------------------------------------------------------------

/// CROSSING LAW, one direction: truth enters value position at an out item, and
/// the adapter is a carrier of its own so the crossing is visible.
#[test]
fn truth_crosses_into_value_position_at_an_out_item() {
    let tree = admits("users(*) |> ((age > 18) as adult)");
    assert_eq!(count::<TruthAsValue>(&tree), 1);
    assert_eq!(count::<Comparison>(&tree), 1);
}

/// NO PEMDAS, structurally: an operand derives no infix form, so a chain needs
/// its parens and the parens are one carrier.
#[test]
fn infix_operands_are_non_infix() {
    let tree = admits("users(*) |> ((a + b) * c)");
    assert_eq!(count::<InfixOperator>(&tree), 2);
    assert_eq!(count::<ParenthesizedOperand>(&tree), 1);
}

/// ONE TEMPLATE PARSE: the CST has one template form and the build classifies
/// it once by content. A zero-interpolation template is still a template here.
#[test]
fn there_is_one_template_form() {
    let interpolated = admits("users(*) |> (:\"hi {name}\" as g)");
    assert_eq!(count::<Template>(&interpolated), 1);
    assert_eq!(count::<Interpolation>(&interpolated), 1);

    let zero = admits("users(*) |> (:\"hi\" as g)");
    assert_eq!(count::<Template>(&zero), 1);
    assert_eq!(count::<Interpolation>(&zero), 0);
}

/// A NAME template and a VALUE template never mix: one is spec material, the
/// other is an expression, and they are different kinds.
#[test]
fn name_templates_and_value_templates_are_different_kinds() {
    let name = admits("users(*) |> *(a as :\"{@}_x\")");
    assert_eq!(count::<AsNameTemplate>(&name), 1);
    assert_eq!(count::<Template>(&name), 0);
}

/// THE HEADER CLASSIFIES: the separator's presence decides anchored versus
/// searched at PARSE, and arm content never reclassifies.
#[test]
fn the_case_header_classifies_at_parse() {
    let anchored = admits("users(*) |> (_:(a @ 1 -> \"x\"; _ -> \"y\") as g)");
    assert_eq!(count::<AnchoredCase>(&anchored), 1);
    assert_eq!(count::<SearchedCase>(&anchored), 0);

    let searched = admits("users(*) |> (_:(a > 1 -> \"x\"; _ -> \"y\") as g)");
    assert_eq!(count::<SearchedCase>(&searched), 1);
    assert_eq!(count::<AnchoredCase>(&searched), 0);
}

/// The sourceless inner form is discriminated from the case sigil by its third
/// character — the leading comma, which is the no-op base made visible.
#[test]
fn the_leading_comma_discriminates_the_sourceless_inner_form() {
    let inner = admits("users(*) |> (_:(, _(1;2) ~> count:(*)) as n)");
    assert_eq!(count::<AnonScalarSubquery>(&inner), 1);
    assert_eq!(count::<CaseLike>(&inner), 0);
}

/// THE BINDER NAMES THE FLOW. A named-binder lambda is legal only where the
/// position supplies the slot.
#[test]
fn the_named_binder_lambda_parses() {
    let tree = admits("users(*) |> $(:(|v| v + 1))(a)");
    assert_eq!(count::<LambdaBinder>(&tree), 1);
    assert_eq!(text_of::<LambdaBinder>(&tree), "|v|");
}

/// The CST admits zero or many holes at any depth and the BUILDER judges the
/// count once. Refusing the second hole here would put one judgment in two
/// places — and refusing the nested one would refuse a lawful composition.
#[test]
fn the_hole_stands_at_any_depth() {
    let nested = admits("users(*) |> $(upper:(trim:(@)))(first_name)");
    assert_eq!(count::<CompositionInput>(&nested), 1);

    let two = admits("users(*) |> $(concat:(@, @))(a)");
    assert_eq!(
        count::<CompositionInput>(&two),
        2,
        "two landings parse; the refusal is the builder's"
    );

    let none = admits("users(*) |> $(upper:(b))(a)");
    assert_eq!(
        count::<CompositionInput>(&none),
        0,
        "hole elision parses; the implicit landing is the builder's"
    );
}

/// THE MODE IS THE COMPRESSION: a column pick on a mode-compressed call is one
/// kind, and the accessor is not.
#[test]
fn a_field_select_is_not_a_json_accessor() {
    let field = admits("users(*) |> (foo:(x).out1)");
    assert_eq!(count::<FieldSelect>(&field), 1);
    assert_eq!(count::<JsonAccess>(&field), 0);

    let json = admits("users(*) |> (x:{.a.b})");
    assert_eq!(count::<JsonAccess>(&json), 1);
    assert_eq!(count::<FieldSelect>(&json), 0);
}

/// A spread expands; a reference does not. They are different carriers in every
/// enumerating position.
#[test]
fn spreads_are_a_carrier_of_their_own() {
    for (src, kind) in [
        ("users(*) |> (e.*)", "glob"),
        ("users(*) |> (/re/)", "regex"),
        ("users(*) |> (|1:3|)", "span"),
    ] {
        let tree = admits(src);
        assert_eq!(count::<Spread>(&tree), 1, "{src} ({kind})");
    }
}

/// polarity is DATA — one carrier for `+` and `\+`, never a variant pair.
#[test]
fn polarity_is_one_carrier() {
    let pos = admits("users(*), +orders(, id = 1)");
    assert_eq!(text_of::<Polarity>(&pos), "+");
    let neg = admits("users(*), \\+orders(, id = 1)");
    assert_eq!(text_of::<Polarity>(&neg), "\\+");
}

/// Membership negates with the keyword; the sigils and the keyword never trade
/// places.
#[test]
fn membership_negates_with_the_keyword() {
    let tree = admits("users(*), a not in (1; 2)");
    assert_eq!(count::<Membership>(&tree), 1);
    assert_eq!(count::<NotKeyword>(&tree), 1);
    assert_eq!(count::<Polarity>(&tree), 0);
}

// ---------------------------------------------------------------------------
// The operator layer
// ---------------------------------------------------------------------------

/// Every pipe operator is its own kind. A cover's guard is per-cell — ruled in,
/// and admitted here.
#[test]
fn the_pipe_operators_are_eight_distinct_kinds() {
    assert_eq!(count::<Project>(&admits("users(*) |> (a)")), 1);
    assert_eq!(count::<ProjectOut>(&admits("users(*) |> -(a)")), 1);
    assert_eq!(count::<Rename>(&admits("users(*) |> *(a as b)")), 1);
    assert_eq!(count::<Embed>(&admits("users(*) |> +(1 as one)")), 1);
    assert_eq!(count::<MapCover>(&admits("users(*) |> $(upper:(@))(a)")), 1);
    assert_eq!(
        count::<EmbedMapCover>(&admits("users(*) |> +$(upper:(@))(a)")),
        1
    );
    assert_eq!(
        count::<Transform>(&admits("users(*) |> $$(upper:(a) as a)")),
        1
    );
    assert_eq!(count::<Group>(&admits("users(*) |> %(a)")), 1);

    let guarded = admits("users(*) |> $(upper:(@))(a | x > 1)");
    assert_eq!(count::<Guard>(&guarded), 1);
}

/// `<~` is one glyph, two carriers: the group's ordered consumption and the
/// window's per-row frame. Related by lowering, never merged in meaning.
#[test]
fn the_window_glyph_has_two_carriers() {
    let delegate = admits("users(*) |> %(a ~> (b) <~ #(c desc))");
    assert_eq!(count::<GroupDelegate>(&delegate), 1);
    assert_eq!(count::<WindowSpec>(&delegate), 0);

    let window = admits("users(*) |> (sum:(a) <~ %(b) #(c) rows(_, .))");
    assert_eq!(count::<WindowSpec>(&window), 1);
    assert_eq!(count::<GroupDelegate>(&window), 0);
}

/// THE SINGLETON PIPE is sugar for the zero-key group, and the CST keeps the
/// authored spelling so normalization is the one place it disappears.
#[test]
fn the_singleton_reduction_keeps_its_spelling() {
    let sugar = admits("users(*) ~> count:(*) as n");
    assert_eq!(count::<SingletonReduction>(&sugar), 1);
    assert_eq!(count::<Group>(&sugar), 0);

    let spelled = admits("users(*) |> %( ~> count:(*) as n)");
    assert_eq!(count::<Group>(&spelled), 1);
}

/// THE IN IS THE HEADING WITNESS: a pivot's operands are both non-infix, and
/// the pivot is admitted in reduction position only.
#[test]
fn a_pivot_is_a_reduction_item() {
    let tree = admits("users(*) |> %(a ~> score of subject)");
    assert_eq!(count::<Pivot>(&tree), 1);
}

// ---------------------------------------------------------------------------
// Effects
// ---------------------------------------------------------------------------

/// Effect calls are read by syntactic POSITION: one group is receipt access,
/// two are (parameters)(receipt access). Neither group contents nor a callee
/// descriptor participates in parsing.
#[test]
fn effect_groups_are_read_by_position() {
    let lower = admits("log!(*)");
    assert_eq!(count::<LowerOrderEffrelex>(&lower), 1);

    let higher = admits("log!(\"x\")(*)");
    assert_eq!(count::<EffrelexInteriorFunctor>(&higher), 1);

    // The `()` in a ground zero-parameter call is a surface marker that
    // normalizes to an omitted ho_part — it never constructs an empty one.
    let empty = admits("f!()(*)");
    assert_eq!(count::<EmptyEffectArguments>(&empty), 1);
    assert_eq!(count::<HoPart>(&empty), 0);
}

/// The unwrap pipe is a PIPE FORM, never a boundary.
#[test]
fn the_unwrap_pipe_is_a_pipe_form() {
    let tree = admits("users(*) !> log!(*)");
    assert_eq!(count::<UnwrapPipeOperator>(&tree), 1);
    assert_eq!(count::<PostPipeEffrelex>(&tree), 1);
    assert_eq!(count::<StageBoundary>(&tree), 0);
}

/// Pure material attaches to an effect chain through the ordinary continuation
/// route; another effect attaches only through a pipe or a connective. The same
/// character appears on both roads and what FOLLOWS it decides.
#[test]
fn one_comma_joins_an_effect_and_the_next_joins_a_predicate() {
    let tree = admits("users(*), log!(+-), age > 3");
    assert_eq!(count::<BinaryConnective>(&tree), 1);
    assert_eq!(count::<CommaContinuation>(&tree), 1);
    assert_eq!(count::<SignedWitness>(&tree), 1);
}

/// A mutation source exists solely to be fed to its terminal, and ONE
/// production carries it — the consuming terminal classifies.
#[test]
fn a_mutation_source_is_one_production() {
    let update = admits("users!!(*), age > 3 |> update!(*)");
    assert_eq!(count::<MutationSource>(&update), 1);
    let delete = admits("users!!(*), age > 3 |> delete!(*)");
    assert_eq!(count::<MutationSource>(&delete), 1);
}

// ---------------------------------------------------------------------------
// Definitions
// ---------------------------------------------------------------------------

/// Rule bodies come in exactly three positions — relational, value, truth — and
/// each has its own carrier.
#[test]
fn the_three_rule_body_positions_are_three_kinds() {
    assert_eq!(
        count::<FoRule>(&admits_file("adults(*) :- users(*), age > 18")),
        1
    );
    assert_eq!(count::<HoRule>(&admits_file("twice(f(*))(*) :- f(*)")), 1);
    assert_eq!(
        count::<FunctionRule>(&admits_file("double:(x) :- x * 2")),
        1
    );
    assert_eq!(count::<ConstantRule>(&admits_file("pi :- 3.14159")), 1);
    assert_eq!(count::<SigmaRule>(&admits_file("big(x) :- x > 10")), 1);
    assert_eq!(count::<EffectRule>(&admits_file("sync!(*) :- log!(*)")), 1);
}

/// A relational clause body is a complete `relex`: the let block a query may
/// write stands in a rule body too, and it belongs to THAT rule.
#[test]
fn a_relational_rule_body_is_a_whole_relex() {
    let tree = admits_file(
        "?- users(*)\n\
         foo(*) :- users(*)\n\
         bar(*) :- foo(*), age < 50\n\
         baz(*) :-\n  bar(*) : iamcte\n  iamcte(*)\n",
    );
    assert_eq!(count::<FoRule>(&tree), 3);
    // ONE binding, and it is inside the third rule's body — not a fourth
    // top-level form the file happened to admit.
    assert_eq!(count::<LabelCte>(&tree), 1);
    let baz = tree
        .root_branch()
        .into_iter()
        .flat_map(|_| delightql_cst::walk(&tree))
        .filter_map(|n| FoRule::cast(n.node()))
        .find(|rule| rule.name().is_some_and(|n| tree.text(n) == "baz"))
        .expect("the third rule");
    let body = baz.body().expect("a rule has a body");
    assert_eq!(count::<LabelCte>(&tree), 1);
    assert!(
        body.let_block().is_some(),
        "the local binding belongs to baz's body: {}",
        tree.text(baz)
    );
    // The higher-order and edge bodies are the same carrier.
    assert!(admits_file("twice(f(*))(*) :- t(*) : c c(*)")
        .root_branch()
        .is_some());
    assert!(admits_file("a(*) &(::ctx) b(*) :- t(*) : c c(*)")
        .root_branch()
        .is_some());
}

/// A definition documents itself in the doc slot after its neck. This is not
/// an annotation — the closed set decorates a POSITION in a chain, and this
/// belongs to one clause — so it has its own node and its own slot.
#[test]
fn a_definition_documents_itself_in_its_doc_slot() {
    for src in [
        "senior_users(*) :- (~~docs Users aged 65 or older. ~~) users(*), age >= 65",
        "double:(x) :- (~~docs Multiplies the input by two. ~~) (x * 2)",
        "twice(f(*))(*) :- (~~docs Runs it twice. ~~) f(*)",
        "pi :- (~~docs Half a turn. ~~) 3.14159",
        "big(x) :- (~~docs Over ten. ~~) x > 10",
        "a(*) &(::ctx) b(*) :- (~~docs The edge. ~~) t(*)",
    ] {
        let tree = admits_file(src);
        assert_eq!(count::<DefinitionDoc>(&tree), 1, "{src}");
        assert_eq!(count::<DocSlot>(&tree), 1, "{src}");
    }

    // The body is opaque prose: `*`, `/` and `!` are text there.
    let tree = admits_file("v(*) :- (~~docs Uses users(*) and f!/g at 100% ~~) users(*)");
    assert_eq!(
        text_of::<DocText>(&tree).trim(),
        "Uses users(*) and f!/g at 100%"
    );

    // It stands in the doc slot and NOWHERE else: not at a continuation
    // anchor, and not in a query.
    support::refuses_query("users(*) (~~docs not here ~~)");
    refuses_file("?- users(*) (~~docs not here ~~)");

    // ONE document per slot, structurally.
    refuses_file("v(*) :- (~~docs one ~~) (~~docs two ~~) users(*)");
}

/// The doc slot's OTHER inhabitants are the annotations that need no relation.
/// An assertion's body is a continuation evaluated against the relation it
/// stands on, and the slot precedes the body — so it has no derivation there,
/// and the same assertion on a continuation anchor is ordinary.
#[test]
fn a_doc_slot_takes_the_annotations_that_need_no_relation() {
    for src in [
        "v(*) :- (~~danger://cardinality/cartesian ~~) users(*)",
        "v(*) :- (~~config://generation/rule/inlining/view ~~) users(*)",
        "v(*) :- (~~error://semantic/constraint ~~) users(*)",
        "v(*) :- (~~danger://cardinality/cartesian ~~) (~~docs why ~~) users(*)",
        "v(*) :- (~~docs why ~~) (~~danger://cardinality/cartesian ~~) users(*)",
    ] {
        assert_eq!(count::<DefinitionAnnotation>(&admits_file(src)), 1, "{src}");
    }

    refuses_file("v(*) :- (~~assert ~> count:(*) as c ~~) users(*)");
    assert_eq!(
        count::<AssertAnnotation>(&admits_file(
            "v(*) :- users(*) (~~assert ~> count:(*) as c ~~)"
        )),
        1
    );
}

/// `(/* … */)` is the other documentation form and stays a different one: a
/// smart comment attaches by position anywhere, and neither spelling is
/// rewritten into the other.
#[test]
fn the_smart_comment_and_the_definition_doc_stay_apart() {
    let smart = admits_file("v(*) :- (/* a note */) users(*)");
    assert_eq!(count::<SmartComment>(&smart), 1);
    assert_eq!(count::<DefinitionDoc>(&smart), 0);

    let doc = admits_file("v(*) :- (~~docs a note ~~) users(*)");
    assert_eq!(count::<DefinitionDoc>(&doc), 1);
    assert_eq!(count::<SmartComment>(&doc), 0);

    // Both, and each still itself.
    let both = admits_file("v(*) :- (~~docs a note ~~) (/* another */) users(*)");
    assert_eq!(count::<DefinitionDoc>(&both), 1);
    assert_eq!(count::<SmartComment>(&both), 1);
}

/// A malformed binding still refuses. The body's let block admits what a
/// query's does and nothing more — an incomplete one is not quietly read as
/// the body itself.
#[test]
fn a_malformed_rule_body_binding_refuses() {
    // No body after the binding: `bar(*) : iamcte` alone is a let block with
    // nothing to bind into.
    refuses_file("baz(*) :-\n  bar(*) : iamcte\n");
    // A head with no body, and a neck with no head.
    refuses_file("baz(*) :-\n  bar(*) :\n  iamcte(*)\n");
    refuses_file("baz(*) :-\n  : iamcte\n  iamcte(*)\n");
}

/// One law for the ':' and ':-' necks: a head term supplies a constant either
/// way, and `as` makes the heading offer.
#[test]
fn head_terms_supply_constants_under_both_necks() {
    let neck = admits_file("adults(\"x\" as tag, id) :- users(*)");
    assert_eq!(count::<HeadTerm>(&neck), 2);

    let cte = admits("adults(\"x\" as tag, id): users(*) adults(*)");
    assert_eq!(count::<HeadTerm>(&cte), 2);
    assert_eq!(count::<StandardCte>(&cte), 1);
}

/// FACT ELABORATION: a fact is not a distinct body kind, and its tabular
/// interior takes the SAME separator shape as every other one.
#[test]
fn one_separator_shape_serves_every_tabular_interior() {
    for src in [
        "colors(name, hex @ \"red\", \"f00\")",
        "colors(name, hex ---- \"red\", \"f00\")",
    ] {
        let tree = admits_file(src);
        assert_eq!(count::<FactBody>(&tree), 1, "{src}");
        assert_eq!(count::<Separator>(&tree), 1, "{src}");
    }
    for src in ["_(a, b @ 1, 2)", "_(a, b ---- 1, 2)"] {
        assert_eq!(count::<AnonBody>(&admits(src)), 1, "{src}");
    }
    // The fact function's arrow head takes it too, in both spellings.
    for src in ["sq(a -> b @ 1 -> 1)", "sq(a -> b ---- 1 -> 1; 2 -> 4)"] {
        assert_eq!(count::<FactFunction>(&admits_file(src)), 1, "{src}");
    }
}

/// A SPARSE FILL IS A DATUM WHEREVER A HEADER MAY DECLARE ONE SPARSE. The
/// fact body reuses `header_row`, so a fact declares sparse columns exactly as
/// an anonymous table does — and the same `_(col @ value)` fill supplies them.
/// A declarable-and-inert mark would be syntax nothing could ever consume.
#[test]
fn a_fact_row_fills_a_sparse_column() {
    let tree = admits_file(
        "config(key, value, note? ---- \"a\", 1 ; \"b\", 2, _(note @ \"why\"))",
    );
    assert_eq!(count::<FactBody>(&tree), 1);
    assert_eq!(count::<SparseMark>(&tree), 1);
    assert_eq!(count::<SparseFill>(&tree), 1);

    // The anonymous relation admits both halves; the fact now spells the same
    // two, not a second vocabulary.
    let anon = admits("_(key, value, note? ---- \"a\", 1 ; \"b\", 2, _(note @ \"why\"))");
    assert_eq!(count::<SparseFill>(&anon), 1);

    // A multi-column fill is the one production's many-item case.
    let many = admits_file(
        "config(key, a?, b? ---- \"x\", _(a, b @ 1, 2))",
    );
    assert_eq!(count::<SparseFill>(&many), 1);
    let fill = first::<SparseFill>(&many);
    assert_eq!(fill.column().count(), 2);
    assert_eq!(fill.value().count(), 2);

    // A FILL'S VALUES ARE GROUND, so admitting the carrier leaves every fact
    // datum a constant: a column reference has no derivation in one.
    refuses_file("config(key, note? ---- \"x\", _(note @ other))");
}

/// A FACT-FUNCTION'S INPUT ARMS ARE GROUND MATCH ROWS. Conditions do not
/// derive there; the searched form is an ordinary function rule whose body is
/// a searched case.
#[test]
fn a_fact_function_arm_matches_and_does_not_test() {
    let matched = admits_file("grade(score -> letter ---- 90 -> \"A\"; _ -> \"F\")");
    assert_eq!(count::<FactFunction>(&matched), 1);

    refuses_file("grade(score -> letter ---- score > 90 -> \"A\"; _ -> \"F\")");

    // The searched form's canonical spelling: a function rule, a case body.
    let searched = admits_file(
        "grade:(score) :- _:(score > 90 -> \"A\"; score > 80 -> \"B\"; _ -> \"F\")",
    );
    assert_eq!(count::<FunctionRule>(&searched), 1);
    assert_eq!(count::<FactFunction>(&searched), 0);
}

/// A fact function declares its mode in both directions: many inputs, many
/// outputs.
#[test]
fn a_fact_function_takes_many_outputs() {
    let tree = admits_file("foo(a, b -> c, d ---- 1, 2 -> 3, 4; _ -> 0, 0)");
    let f = first::<FactFunction>(&tree);
    assert_eq!(f.inputs().count(), 2);
    assert_eq!(f.outputs().count(), 2);
    assert_eq!(count::<FactDefault>(&tree), 1);
}

/// AN EDGE DECLARATION IS A GROUND HEAD, and only '&' declares.
#[test]
fn an_edge_declaration_uses_the_selection_operator() {
    let tree = admits_file("users_t(*) &(::normal) orders_t(*) :- users(*)");
    assert_eq!(count::<EdgeDeclaration>(&tree), 1);
    assert_eq!(count::<EdgeTerm>(&tree), 2);
}

/// THE LIFT'S COST: '&' bounds arguments and ';' separates lifted rows, and an
/// anon table reaches an argument row directly — the lift is sugar for it.
#[test]
fn the_lift_and_the_anon_table_are_the_same_argument_row() {
    let lifted = admits("f(users(*) & 1, 2; 10, 20)(*)");
    assert_eq!(count::<LiftSigil>(&lifted), 1);
    assert_eq!(count::<HoPart>(&lifted), 1);

    let spelled = admits("f(users(*), _(1, 2; 10, 20))(*)");
    assert_eq!(count::<HoPart>(&spelled), 1);
    assert_eq!(count::<AnonGrelex>(&spelled), 1);

    // And ';' alone lifts, with no arguments to bound.
    assert_eq!(count::<HoPart>(&admits("f(\"a\";\"b\")(*)")), 1);
}

// ---------------------------------------------------------------------------
// Annotations and roots
// ---------------------------------------------------------------------------

/// THE SET IS CLOSED. Five lawful annotations plus the reserved room that must
/// parse so its refusal can teach.
#[test]
fn the_annotation_set_is_the_closed_one() {
    assert_eq!(
        count::<AssertAnnotation>(&admits("users(*) (~~assert |> (a) ~~)")),
        1
    );
    assert_eq!(
        count::<ErrorAnnotation>(&admits("users(*) (~~error://unbound/x ~~)")),
        1
    );
    assert_eq!(
        count::<DangerAnnotation>(&admits("users(*) (~~danger://drop/table ~~)")),
        1
    );
    assert_eq!(
        count::<ConfigAnnotation>(&admits("users(*) (~~config://strategy/x 1 ~~)")),
        1
    );
    assert_eq!(
        count::<DdlAnnotation>(&admits("(~~ddl a(*) :- b(*) ~~) users(*)")),
        1
    );
    assert_eq!(
        count::<ReservedAnnotation>(&admits("users(*) (~~emit://file/out ~~)")),
        1,
        "reserved room parses so the refusal can name the effect algebra"
    );
}

/// An annotation decorates a position and never changes the relex around it:
/// the chain either side of it is the same chain.
#[test]
fn an_annotation_is_not_a_continuation() {
    let bare = admits("users(*), age > 3");
    let decorated = admits("users(*) (/* why */), age > 3");
    assert_eq!(
        count::<Continuation>(&bare),
        count::<Continuation>(&decorated)
    );
}

/// The companion cell's root is selected by COLUMN. Nothing reads the cell to
/// decide what it is.
#[test]
fn the_companion_column_selects_the_root() {
    let mut p = Parser::new();

    let pk = p.parse_companion_cell(CompanionColumn::Constraint, "%%(order_id, product_id)");
    assert!(!pk.has_defects(), "{:?}", pk.defects());
    assert_eq!(count::<PrimaryKeySigil>(&pk), 1);

    let check = p.parse_companion_cell(CompanionColumn::Constraint, "@ > 0");
    assert!(!check.has_defects(), "{:?}", check.defects());
    assert_eq!(count::<ConstraintTruth>(&check), 1);

    let default = p.parse_companion_cell(CompanionColumn::Default, "datetime:(\"now\")");
    assert!(!default.has_defects(), "{:?}", default.defects());
    assert_eq!(count::<DefaultCell>(&default), 1);

    // The same bytes under the other column reach the other root — which is the
    // whole reason the column, not the content, decides.
    let as_constraint = p.parse_companion_cell(CompanionColumn::Constraint, "%");
    assert_eq!(count::<UniqueKeySigil>(&as_constraint), 1);
}

/// A preamble of any length parses. Two repeat nonterminals over the same
/// alternatives — a pure let block and an effect one — fork at every item and
/// the GLR stacks multiply, which cost the fifth binding in a query. ONE let
/// block is the fix, and this is the property that says so: the corpus has
/// queries with a dozen bindings and they must not depend on their count.
#[test]
fn a_preamble_does_not_have_a_length_limit() {
    for n in [1, 5, 12, 30] {
        let labels: String = (0..n)
            .map(|i| format!("_(x@{i}) : c{i}\n"))
            .collect::<String>();
        let tree = admits(&format!("{labels}zz(*)"));
        assert_eq!(count::<LabelCte>(&tree), n);

        let heads: String = (0..n)
            .map(|i| format!("c{i}(*): _(x@{i})\n"))
            .collect::<String>();
        assert_eq!(count::<StandardCte>(&admits(&format!("{heads}zz(*)"))), n);

        let cfes: String = (0..n).map(|i| format!("f{i}:(x): x + {i}\n")).collect();
        assert_eq!(count::<Cfe>(&admits(&format!("{cfes}zz(*)"))), n);
    }
}

/// A let block carrying an effect binding is the SAME block. Whether an effect
/// CTE may stand in a pure query is a judgment over the built block, not a
/// second preamble production that would fork against the first at every item.
#[test]
fn the_let_block_is_one_production() {
    let pure = admits("adults(*): users(*) adults(*)");
    assert_eq!(count::<LetBlock>(&pure), 1);

    let effectful = admits("t!(*): users(*) t(*) |> log!(*)");
    assert_eq!(count::<LetBlock>(&effectful), 1);
    assert_eq!(count::<EffectStandardCte>(&effectful), 1);
}

/// THE WRITTEN NAME IS THE NAMING (FN.12): a slot binds by POSITION and
/// publishes no name for `as` to change. The form is recognized under its own
/// production so the ruled teaching can name the alias the author wrote and
/// point at the projection that renames — which is a judgment about POSITION,
/// and only the typed shape knows the position. A token scan could not: `as`
/// is lawful in a CTE head, a projection, a head term and fact data, and the
/// characters do not say which parens they stand in.
///
/// Nothing normalizes it. The refusal is
/// `semantic/constraint/positional_alias`, made once, on the compiler side.
#[test]
fn a_renamed_slot_is_recognized_for_its_teaching() {
    let named = admits("actor(actor_id as aid, x)");
    assert_eq!(count::<RenamedSlot>(&named), 1);

    // Every slot kind, because "in every slot" is the ruling's word.
    assert_eq!(count::<RenamedSlot>(&admits("actor(1 as first, x)")), 1);
    assert_eq!(count::<RenamedSlot>(&admits("actor(upper:(n) as u, x)")), 1);
    assert_eq!(count::<RenamedSlot>(&admits("actor(_ as anything, x)")), 1);

    // THE ANON HEADER IS A SLOT ROW — the caller-pattern slot law, verbatim
    // (FN.14) — so it reaches the same production and the same one refusal.
    assert_eq!(count::<RenamedSlot>(&admits("_(a as b @ 1)")), 1);

    // The positions where `as` DOES name stay untouched: a slot's alias is
    // recognized, and nothing else was widened to admit one.
    assert_eq!(count::<RenamedSlot>(&admits("c(1 as x): users(*) c(*)")), 0);
    assert_eq!(count::<RenamedSlot>(&admits("users(*) |> (id as x)")), 0);
}

/// The three roots are three branches of one entry point.
#[test]
fn the_roots_are_branches_of_one_entry_point() {
    assert_eq!(
        admits_file("?- users(*)\nadults(*) :- users(*)").entrance(),
        Root::DefinitionFile
    );
    assert_eq!(
        admits("users(*)\norders(*)").entrance(),
        Root::QuerySequence
    );
}
