// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The term specification and its canonicalizer.
//!
//! A TERM is the interior of a mention — the thing a reference literal
//! names without dereferencing. The committed extent is table functors
//! only: a single relation-access term (`people(*)`,
//! `people(, age >= 30)`, `orders(id, _, total)`). Namespace paths,
//! function terms, and every other candidate kind are admitted by
//! ruling, never by drift; until admitted they refuse here.
//!
//! Two terms are identical exactly when their canonical spellings are
//! byte-equal. Canonicalization normalizes the lexical and nothing
//! else: whitespace, and identifier case exactly where the language
//! folds it (unstropped identifiers ASCII-fold; a strop's interior is
//! uninterpreted bytes and never folds — the same line `SqlIdentifier`
//! equality draws). It never normalizes semantics:
//! `people(, age >= 30)` and `people(, 30 <= age)` are different
//! terms, deliberately — spelling is identity.
//!
//! The canonicalizer is the format engine, called in-process under its
//! frozen default style — no CLI, no config file, no knob enters
//! adjudication. Its output serves as both the match key (head
//! grounding compares canonical spellings) and the storage format (the
//! edge catalog holds naked canonical spellings): one function, two
//! consumers, no drift possible between them. The laws that let a
//! formatter adjudicate identity — determinism, idempotence,
//! round-trip — are pinned in this module's tests.

use crate::error::{DelightQLError, Result};
use crate::pipeline::syntax::{cst, TypedNode};

fn not_a_term(detail: String) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "mention/term/not_a_term",
        format!(
            "the term specification admits table functors only — a single relation-access term such as people(*), people(, age >= 30), or orders(id, _, total); {detail}"
        ),
        "namespace paths, function terms, pipelines, and joins are not terms; new term kinds are admitted by ruling",
    )
}

fn unformattable(detail: String) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "mention/term/unformattable",
        format!("the canonicalizer cannot emit this term: {detail}"),
        "the term parses but the format engine takes no position on part of it — unformatted bytes never become a match key or a stored spelling",
    )
}

/// The canonical interior of a delimited-mention token (`` :`term` ``),
/// or refuse. Shared by every consumer of the raw token — value
/// position and rule heads — so the identifier-interior refusal and
/// the canonicalization happen once, identically: an identifier
/// interior refuses toward the light spelling (identifier terms are
/// not yet admitted, and while they are not, `::name` is the only
/// identifier mention); anything else must canonicalize as a term.
pub(crate) fn mention_interior_from_token(token_text: &str) -> Result<String> {
    let interior = token_text
        .strip_prefix(":`")
        .and_then(|t| t.strip_suffix('`'))
        .ok_or_else(|| DelightQLError::parse_error("malformed delimited mention"))?;
    let trimmed = interior.trim();
    let identifier_shaped = !trimmed.is_empty()
        && trimmed
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_');
    if identifier_shaped {
        return Err(DelightQLError::validation_error_categorized(
            "mention/identifier_interior",
            format!(
                "an identifier mention is spelled ::{trimmed} — the delimited spelling is for terms, and identifier terms are not yet admitted by the term specification"
            ),
            "write the light spelling; the delimited form may admit identifier terms later, by ruling",
        ));
    }
    canonicalize_term(interior)
}

/// Canonicalize a term, or refuse.
///
/// Parses the input, gates it against the term specification (a single
/// relation-access term and nothing else), folds identifier case
/// exactly where the language folds it, and emits through the format
/// engine under the frozen default style. The returned bytes ARE the
/// term's identity: byte-equal canonical spellings, same term.
pub fn canonicalize_term(source: &str) -> Result<String> {
    let tree = crate::pipeline::parse::query_sequence(source)
        .map_err(|e| not_a_term(format!("this input does not parse: {e}")))?;

    // THE SHAPE GATE: one chain, whose head is a named relation access and
    // whose body carries nothing else. A second statement, a preamble, a pipe
    // or join continuation, or an annotation all add a member here and refuse.
    if !is_single_relation_access(&tree, source) {
        return Err(not_a_term(
            "this input parses, but not as a single relation-access term".to_string(),
        ));
    }

    emit_canonical(&tree, unformattable)
}

/// THE CANONICAL SPELLING OF ONE QUERY BODY.
///
/// The same canonicalizer, at the other position that needs one: a consulted
/// file's relational goal, whose ledger row names it. The term SHAPE GATE
/// does not run here — a goal body is a whole relex, not a mention's
/// interior — but the identity law is the same one, and it has to be, or two
/// spellings of one goal would be one thing to the compiler and two to the
/// ledger.
///
/// `refuse` builds the caller's own refusal, so a goal that cannot be
/// canonicalized says so in the caller's vocabulary rather than a term's.
pub(crate) fn canonicalize_query(
    source: &str,
    refuse: impl Fn(String) -> DelightQLError,
) -> Result<String> {
    let tree = crate::pipeline::parse::query_sequence(source)
        .map_err(|e| refuse(format!("it does not parse on its own: {e}")))?;
    emit_canonical(&tree, refuse)
}

/// Fold, then emit through the format engine under the frozen default style.
/// A pass-through is a refusal, never a silent identity — keeping the raw
/// bytes would be a second canonicalizer that agrees with this one only by
/// accident.
fn emit_canonical(
    tree: &crate::pipeline::syntax::SyntaxTree,
    refuse: impl Fn(String) -> DelightQLError,
) -> Result<String> {
    let folded = fold_unstropped_identifiers(tree);
    let config = delightql_formatter::FormatConfig::default();
    match delightql_formatter::format_outcome(&folded, &config)
        .map_err(|e| refuse(e.to_string()))?
    {
        delightql_formatter::FormatOutcome::Formatted(text) => {
            Ok(text.strip_suffix('\n').unwrap_or(&text).to_string())
        }
        delightql_formatter::FormatOutcome::PassedThrough { reason, .. } => Err(refuse(format!(
            "the format engine passed it through ({reason:?})"
        ))),
    }
}

/// ASCII-lowercase every unstropped identifier, and nothing else.
///
/// This is the one place the language folds: an unstropped identifier carries
/// no child, a stropped one carries its `stropped_form` and its bytes are
/// uninterpreted — as are string and number literals. ASCII-lowercasing is
/// byte-preserving, so every other range stays valid.
fn fold_unstropped_identifiers(tree: &crate::pipeline::syntax::SyntaxTree) -> String {
    let mut bytes = tree.source().as_bytes().to_vec();
    for node in crate::pipeline::syntax::walk(tree) {
        let Some(identifier) = cst::Identifier::cast(node.node()) else {
            continue;
        };
        if identifier.child().is_some() {
            continue;
        }
        if let Some(range) = tree.byte_range(identifier) {
            for b in &mut bytes[range] {
                *b = b.to_ascii_lowercase();
            }
        }
    }
    String::from_utf8(bytes).expect("ASCII folding preserves UTF-8")
}

/// Whether the submission is exactly one chain headed by a named relation
/// access, with no preamble and no continuation.
fn is_single_relation_access(tree: &crate::pipeline::syntax::SyntaxTree, source: &str) -> bool {
    use crate::pipeline::syntax::cst;
    let Some(cst::SourceFileChild::QuerySequenceRoot(root)) = tree.root_branch() else {
        return false;
    };
    let mut forms = root.children().filter_map(|child| match child {
        cst::QuerySequenceRootChild::QuerySequence(sequence) => Some(sequence),
        cst::QuerySequenceRootChild::QuerySequenceHeader(_) => None,
    });
    let Some(sequence) = forms.next() else {
        return false;
    };
    if forms.next().is_some() {
        return false;
    }
    let mut members = sequence.children();
    let Some(cst::QuerySequenceChild::Relex(relex)) = members.next() else {
        return false;
    };
    if members.next().is_some() || relex.let_block().is_some() {
        return false;
    }
    let Some(body) = relex.body() else {
        return false;
    };
    // A NAMED grelex and nothing beside it: the other members of
    // `let_free_relex` are continuations and annotations, and a term carries
    // neither.
    if body.children().next().is_some() {
        return false;
    }
    if !matches!(body.grelex(), Some(cst::Grelex::NamedGrelex(_))) {
        return false;
    }
    // THE TERM IS THE WHOLE SUBMISSION. A comment is a grammar EXTRA and so
    // leaves no member to count, but it is still text the term does not
    // contain — and it survives formatting, so a canonical form built from it
    // would not be a term. Nothing outside the form's own extent may be
    // anything but whitespace.
    let Some(extent) = tree.byte_range(relex) else {
        return false;
    };
    source[..extent.start].trim().is_empty() && source[extent.end..].trim().is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every input the laws quantify over. Spellings vary in
    /// whitespace and identifier case; strops, string literals, and
    /// number literals appear so the fold's no-touch zones are
    /// exercised.
    const TERMS: &[&str] = &[
        "people(*)",
        "people( * )",
        "PEOPLE(*)",
        "people(, age >= 30)",
        "people(,age>=30)",
        "PEOPLE(, AGE >= 30)",
        "orders(id, _, total)",
        "orders( ID ,  _ , TOTAL )",
        "members(, `Weird Col` >= 1)",
        "people(, name = \"Boston\")",
        "people(, total = 30.50)",
        "people(, 30 <= age)",
    ];

    fn canon(s: &str) -> String {
        canonicalize_term(s).unwrap_or_else(|e| panic!("corpus term {s:?} refused: {e}"))
    }

    /// The AUTHORED token stream of a parse — the byte-level witness of
    /// parsed-term equality. Nothing is normalized here: a strop's case IS
    /// identity, and a comparison that lowercased it would agree about terms
    /// the canonicalizer keeps apart.
    fn leaf_tokens(source: &str) -> Vec<String> {
        let tree = crate::pipeline::parse::query_sequence(source)
            .unwrap_or_else(|e| panic!("term {source:?} does not parse: {e}"));
        tree.tokens().into_iter().map(|token| token.text).collect()
    }

    /// The input under the canonicalizer's OWN fold, so the round-trip law
    /// compares like with like rather than against a second, looser rule.
    fn folded(source: &str) -> String {
        let tree = crate::pipeline::parse::query_sequence(source)
            .unwrap_or_else(|e| panic!("term {source:?} does not parse: {e}"));
        fold_unstropped_identifiers(&tree)
    }

    /// LAW 1 — determinism: same input bytes, same output bytes,
    /// across independent invocations.
    #[test]
    fn law_determinism() {
        for t in TERMS {
            assert_eq!(canon(t), canon(t), "nondeterministic canonical for {t:?}");
        }
    }

    /// LAW 2 — idempotence: a canonical term is its own fixed point,
    /// byte-testable with no oracle at all.
    #[test]
    fn law_idempotence() {
        for t in TERMS {
            let once = canon(t);
            let twice = canon(&once);
            assert_eq!(once, twice, "canonical of {t:?} is not a fixed point");
        }
    }

    /// LAW 3 — round-trip: the canonical spelling parses back to the
    /// same term (leaf token stream, identifiers compared folded).
    /// This is what makes byte-identity a theorem: two different terms
    /// cannot collide onto one canonical spelling, because those bytes
    /// would have to parse back as both.
    #[test]
    fn law_round_trip() {
        for t in TERMS {
            let canonical = canon(t);
            assert_eq!(
                leaf_tokens(&canonical),
                leaf_tokens(&folded(t)),
                "canonical of {t:?} parses back as a different term"
            );
        }
    }

    /// Lexical variation collapses: whitespace and unstropped
    /// identifier case are spelling, not identity. Each class states
    /// its canonical bytes explicitly.
    #[test]
    fn equivalence_classes_collapse() {
        let classes: &[(&str, &[&str])] = &[
            ("people(*)", &["people(*)", "people( * )", "PEOPLE(*)"]),
            (
                "people(, age >= 30)",
                &[
                    "people(, age >= 30)",
                    "people(,age>=30)",
                    "PEOPLE(, AGE >= 30)",
                ],
            ),
            (
                "orders(id, _, total)",
                &["orders(id, _, total)", "orders( ID ,  _ , TOTAL )"],
            ),
            (
                "members(, `Weird Col` >= 1)",
                &[
                    "members(, `Weird Col` >= 1)",
                    "MEMBERS(,   `Weird Col` >= 1)",
                ],
            ),
        ];
        for (expected, members) in classes {
            for m in *members {
                assert_eq!(&canon(m), expected, "class member {m:?}");
            }
        }
    }

    /// Semantic equivalence never collapses: operand order, string
    /// case, number spelling, and strop case are all identity.
    #[test]
    fn semantics_never_normalize() {
        let pairs: &[(&str, &str)] = &[
            ("people(, age >= 30)", "people(, 30 <= age)"),
            ("people(*)", "people(, age >= 30)"),
            ("people(, name = \"Boston\")", "people(, name = \"boston\")"),
            ("people(, total = 30.50)", "people(, total = 30.5)"),
            ("members(, `Weird Col` >= 1)", "members(, `weird col` >= 1)"),
        ];
        for (a, b) in pairs {
            assert_ne!(
                canon(a),
                canon(b),
                "distinct terms {a:?} and {b:?} collided"
            );
        }
    }

    /// Everything outside the committed extent refuses with the
    /// table-functors-only teaching: bare identifiers, namespace
    /// paths, pipelines, joins, multiple statements, comments, and
    /// emptiness are not terms.
    #[test]
    fn non_terms_refuse() {
        let non_terms: &[&str] = &[
            "members",
            "ns::people(*)",
            "members(*) |> (name)",
            "members(*), orders(*)",
            "people(*)\npeople(*)",
            "",
            "people(*) // trailing comment",
        ];
        for nt in non_terms {
            match canonicalize_term(nt) {
                Ok(c) => panic!("non-term {nt:?} canonicalized to {c:?}"),
                Err(e) => {
                    let msg = e.to_string();
                    assert!(
                        msg.contains("table functors only"),
                        "non-term {nt:?} refused without the teaching: {msg}"
                    );
                }
            }
        }
    }
}
