// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! Refusals that belong at the PARSER boundary.
//!
//! A refusal a builder performs is a refusal a consumer can forget to run.
//! Everything below is refused structurally: there is no production to derive
//! it, so the form cannot reach any consumer at all. Each case names the law
//! that draws the line, and each pairs an unlawful spelling with the lawful
//! one it is a near-miss of.
//!
//! Refusals that need a JUDGMENT — hole counting, arity, groundness, order
//! consumption, whether an interior ends in a compression — are deliberately
//! absent. Those are the builder's, and pre-empting them here would put a
//! decision in two places.

mod support;

use support::refuses;

/// ONE GLYPH PER COMPOSITION FAMILY. The ordinary pipe lands in the final
/// formal, so a second relational pipe has no direction left to name and a
/// second function pipe has nothing left to say. Neither spelling derives;
/// an exceptional non-final landing spells with `@`.
#[test]
fn there_is_one_pipe_glyph_per_composition_family() {
    refuses("users(*) |>> (a)");
    refuses("users(*) |> (a /->> f:(@))");
}

/// THE ONE ACCESSOR takes exactly one path, spelled with its steps: `:[1]`
/// reads as a type re-declaration where an accessor only READS.
#[test]
fn the_accessor_takes_one_path_spelling() {
    refuses("users(*) |> (arr:[1])");
}

/// `::` separates NAMESPACE segments. The metadata sigil is `:~>`, with
/// interior whitespace carrying no meaning; admitting `::` for it as well
/// would make `a::b` two constructs at once.
#[test]
fn the_namespace_separator_is_not_the_metadata_sigil() {
    refuses("e(*) ~> title:: { fn } as by_title");
    refuses("e(*) |> %(~> country::status::{ fn })");
}

/// THE SET IS CLOSED: a generic annotation refuses. The five lawful ones and
/// the reserved room are the whole set.
#[test]
fn a_generic_annotation_refuses() {
    refuses("users(*) (~~this_is_not_a_thing ~~)");
    refuses("users(*) (~~whatever some text ~~)");
}

/// A bare value where a predicate stands refuses: the test must be spelled,
/// because a value is not a truth.
#[test]
fn a_bare_value_is_not_a_predicate() {
    refuses("actor(*), last_name");
    refuses("actor(*), 3");
}

/// NO CASE IS A CALLABLE: a case is always a complete expression with its
/// operand present, so it cannot cover a column.
#[test]
fn a_case_is_not_a_cover_callable() {
    refuses("users(*) |> $(_:(a @ 1 -> 2))(c)");
    refuses("users(*) |> +$(_:(a @ 1 -> 2))(c)");
}

/// A bare identifier is not a callable either: `open_functor` spells its `:(`,
/// so a cover over a name alone is underivable.
#[test]
fn a_bare_name_is_not_a_cover_callable() {
    refuses("users(*) |> $(upper)(c)");
}

/// THE HEADER CLASSIFIES: a case whose header says anchored cannot carry a
/// condition arm, and the refusal is at parse rather than a content check.
#[test]
fn a_mixed_case_refuses() {
    refuses("users(*) |> (_:(a @ 1 -> \"x\"; b > 2 -> \"y\") as g)");
    refuses("users(*) |> (_:(a > 1 -> \"x\"; 2 -> \"y\") as g)");
}

/// A PATH IS SPEC, NOT A VALUE: it travels only into positions that APPLY it to
/// a source. It is never an argument — not to a builtin, not to a CFE.
#[test]
fn a_path_is_never_an_argument() {
    refuses("users(*) |> (json_extract:(doc, .a.b))");
    refuses("get:(d, p) : d:{p} users(*) |> (get:(doc, .a.b))");
}

/// A bound has ONE home — the comma member. The postfix and pipe spellings
/// refuse.
#[test]
fn a_bound_has_one_home() {
    refuses("users(*) #<3");
    refuses("users(*) |> #<3");
}

/// Naming on a spread refuses: it expands to many, and bulk renames are the
/// rename cover's job.
#[test]
fn a_spread_cannot_be_named() {
    refuses("users(*) |> (* as x)");
    refuses("users(*) |> (e.* as x)");
}

/// The sigma rule's body is TRUTH material. A domain expression there is a
/// category error, refused structurally rather than accepted and mis-read.
#[test]
fn a_sigma_body_is_truth_material() {
    refuses("p(x) :- users");
    refuses("p(x, y) :- 3 + 4");
}

/// A passthrough DML target is never legal: the engine's catalog is the
/// engine's, and a mutation target takes a predicate identifier alone. The pair
/// is unconstructible rather than checked.
#[test]
fn an_engine_reference_is_never_a_mutation_target() {
    refuses("main/x!!(*), a = 1 |> update!(*)");
}

/// An engine reference is never an effect name either.
#[test]
fn an_engine_reference_is_never_an_effect_name() {
    refuses("main/log!(*)");
}

/// Construction `{_}` refuses; the anaphor is pattern-side only, and the
/// refusal teaches `count:(*)`.
#[test]
fn the_anaphor_is_pattern_side_only() {
    refuses("users(*) ~> {_}");
    refuses("users(*) |> %( ~> {_} as g)");
}

/// The metadata inducer is reduction-side: a bare metadata group inside a
/// constructor is not a construction member.
#[test]
fn a_metadata_group_is_not_a_construction_member() {
    refuses("users(*) |> ({\"inner\": ~> k: ~> {v}} as g)");
}

/// A relex begins with a grelex. An effect is legal only as a direct operand of
/// a chain join — never in an enclosed position, where its ordering would be
/// undefined.
#[test]
fn effects_are_fenced_out_of_enclosed_positions() {
    refuses("users(*), +log!(*)");
    refuses("users(*) |> (log!(*))");
    refuses("f(log!(*))(*)");
}

/// A pattern member is not a record member: the side is fixed by the enclosing
/// curly, so a side-illegal member is a parse error rather than a builder
/// check.
#[test]
fn the_two_curly_sides_do_not_accept_each_others_members() {
    // Path members and the disregarded anaphor are pattern-side.
    refuses("users(*) |> ({.a.b} as g)");
    // A keyed VALUE is construction-side; a pattern binds a name there.
    refuses("users(*), doc ~= {\"k\": 1 + 2}");
}

/// A bare relex is not a definition file entry: every query in the canonical
/// form begins with `?-`.
#[test]
fn the_canonical_form_requires_the_goal_marker() {
    refuses("adults(*) :- users(*)\nadults(*)");
}

/// THE SPEC IS ENCLOSED BY THE CALL IT WINDOWS: `f:(args <~ spec)`. A spec
/// after the closing paren belongs to no call and has no derivation — in
/// value position and in cover position alike. The group delegate's bare
/// sigil is a different carrier and stays.
#[test]
fn a_window_spec_stands_inside_its_calls_parens() {
    refuses("users(*) |> (row_number:() <~ as rn)");
    refuses("users(*) |> (row_number:() <~ #(id) as rn)");
    refuses("users(*) |> (sum:(total) <~ %(d), #(c) as t)");
    refuses("users(*) |> +$(row_number:() <~ #(id) as :\"{@}_rn\")(balance)");
    // A comma stands only BETWEEN two written spec items.
    refuses("users(*) |> (sum:(total <~ %(d),) as t)");
}

/// NAMING IS ONE ACT: the embed-map cover names its column with the same
/// `as` the rename cover spells before a name template. A template standing
/// bare after the callable has no derivation.
#[test]
fn an_embed_map_cover_names_with_as() {
    refuses("users(*) |> +$(upper:() :\"{@}_upper\")(first_name)");
    refuses("users(*) |> +$(:( @ * 2) :\"{@}_x2\")(balance)");
}
