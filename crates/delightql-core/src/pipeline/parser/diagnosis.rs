// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Parse-failure diagnosis: teaching errors mined from the RECOVERY tree.
//!
//! tree-sitter returns a tree even when the parse fails, and the tokens
//! the user typed survive inside and around the ERROR nodes even when
//! the shape around them is unreliable. Grammar-enforced rules can only
//! ever produce grammar-grade errors ("expected valid syntax near …"),
//! so the rules users most need taught — no operator precedence, no
//! `is null`, `_(` as one token — are exactly the ones the parser
//! cannot explain. This pass runs AFTER a parse has failed, keys every
//! pattern on TOKEN PRESENCE (never on recovery shape, which shifts
//! across grammar regenerations), and speaks only when unambiguous — a
//! specific wrong hint is worse than a generic true one (the "needs
//! spaces" lesson). Each diagnosis mints its own
//! badge under parse/ so it is explainable and annotation-matchable.

use crate::pipeline::cst::CstNode;

pub(super) struct ParseDiagnosis {
    pub subcategory: &'static str,
    pub message: String,
}

struct Token {
    text: String,
    start: usize,
    end: usize,
}

const INFIX_OPS: &[&str] = &["+", "-", "*", "/", "%"];
/// Tokens that end an "expression window" — two infix operators are only
/// a PONY violation when nothing on this list separates them.
const WINDOW_BREAKERS: &[&str] = &["(", ")", ",", ";", "|>", "~>", ":", "|", "{", "}", "[", "]"];

pub(super) fn diagnose_failed_parse(root: &CstNode, source: &str) -> Option<ParseDiagnosis> {
    let mut tokens: Vec<Token> = Vec::new();
    collect_leaf_tokens(root, &mut tokens);
    tokens.sort_by_key(|t| t.start);

    // Dash-comment MUST run first: comment text is arbitrary prose, so a
    // `-- check x is null` line would otherwise feed the other patterns
    // words they would misread as the user's query.
    diagnose_dash_comment(&tokens, source)
        .or_else(|| diagnose_sort_minus(&tokens))
        .or_else(|| diagnose_is_null(&tokens, source))
        .or_else(|| diagnose_anon_space(&tokens))
        .or_else(|| diagnose_pony(&tokens))
}

/// Collect leaf tokens — INCLUDING the text recovery dropped. Tree-sitter
/// often represents the offending tokens as nothing at all: a childless
/// ERROR node whose SPAN covers them (`* 2 + 1` is one such leaf), or a
/// gap between an ERROR node's surviving children (`is` between `x` and
/// `null`). The bytes are still there; re-lex every uncovered span so the
/// patterns see what the user actually typed.
fn collect_leaf_tokens(node: &CstNode, out: &mut Vec<Token>) {
    let raw = node.raw_node();
    let children: Vec<CstNode> = node.all_children().collect();

    if children.is_empty() {
        if raw.is_error() || raw.is_missing() {
            lex_span(node.text(), raw.start_byte(), out);
        } else {
            let text = node.text().to_string();
            if !text.trim().is_empty() {
                out.push(Token {
                    text,
                    start: raw.start_byte(),
                    end: raw.end_byte(),
                });
            }
        }
        return;
    }

    if raw.is_error() {
        let base = raw.start_byte();
        let text = node.text();
        let mut pos = 0usize;
        for child in &children {
            let cs = child.raw_node().start_byte() - base;
            if cs > pos {
                lex_span(&text[pos..cs], base + pos, out);
            }
            pos = child.raw_node().end_byte() - base;
        }
        if text.len() > pos {
            lex_span(&text[pos..], base + pos, out);
        }
    }
    for child in &children {
        collect_leaf_tokens(child, out);
    }
}

/// Minimal lexer for recovery-dropped text: words, numbers, and
/// single-character symbols. Enough for token-presence patterns; never
/// used on text the tree parsed successfully.
fn lex_span(text: &str, base: usize, out: &mut Vec<Token>) {
    // char_indices, not a byte walk: recovery text is user-controlled
    // and slicing at a mid-codepoint byte index panics.
    let mut iter = text.char_indices().peekable();
    while let Some((start, c)) = iter.next() {
        if c.is_whitespace() {
            continue;
        }
        let mut end = start + c.len_utf8();
        if c.is_alphanumeric() || c == '_' {
            while let Some(&(i, next)) = iter.peek() {
                if next.is_alphanumeric() || next == '_' {
                    end = i + next.len_utf8();
                    iter.next();
                } else {
                    break;
                }
            }
        }
        out.push(Token {
            text: text[start..end].to_string(),
            start: base + start,
            end: base + end,
        });
    }
}

/// `--` — SQL's line comment, which DelightQL does not have: it lexes as
/// two `-` operators and breaks the parse. Keyed on an ADJACENT minus
/// pair (`a - -b` has a gap and stays silent); a pair behind an odd
/// number of `"` is inside a string literal and stays silent too.
fn diagnose_dash_comment(tokens: &[Token], source: &str) -> Option<ParseDiagnosis> {
    for pair in tokens.windows(2) {
        if pair[0].text == "-" && pair[1].text == "-" && pair[1].start == pair[0].end {
            let quotes_before = source[..pair[0].start].matches('"').count();
            if quotes_before % 2 == 1 {
                continue; // inside a string literal — ordinary text
            }
            return Some(ParseDiagnosis {
                subcategory: crate::uri_registry::subcat::PARSE_COMMENT,
                message: "`--` is not a comment in DelightQL — it lexes as two `-` \
                          operators. Line comments are `//`. (If subtraction of a \
                          negative was meant, group it: `a - (-b)`.)"
                    .to_string(),
            });
        }
    }
    None
}

/// `#(-col)` — minus-prefix descending sort, which DelightQL does not
/// have; the spelling is `#(col desc)`. Keyed on `-` directly after the
/// sort sigil's `(`; a minus deeper in the window (`#(0 - col)`) is
/// arithmetic and stays silent.
fn diagnose_sort_minus(tokens: &[Token]) -> Option<ParseDiagnosis> {
    for w in tokens.windows(3) {
        if w[0].text == "#" && w[1].text == "(" && w[2].text == "-" {
            return Some(ParseDiagnosis {
                subcategory: crate::uri_registry::subcat::PARSE_SORT_MINUS,
                message: "`#(-col)` is not descending sort — the spelling is \
                          `#(col desc)`, per key: `#(a desc, b)`. (Unary minus \
                          as arithmetic needs grouping: `#((0 - col))`.)"
                    .to_string(),
            });
        }
    }
    None
}

/// `col is null` / `col is not null` — SQL spelling with no DelightQL
/// counterpart; `=` is the null-safe equality.
fn diagnose_is_null(tokens: &[Token], source: &str) -> Option<ParseDiagnosis> {
    for (i, tok) in tokens.iter().enumerate() {
        if !tok.text.eq_ignore_ascii_case("is") {
            continue;
        }
        let next = tokens.get(i + 1)?;
        let (negated, null_tok) = if next.text.eq_ignore_ascii_case("not") {
            (true, tokens.get(i + 2))
        } else {
            (false, Some(next))
        };
        // `is` may also lex glued to a following word only as separate
        // leaves; require the literal `null` to follow.
        if let Some(null_tok) = null_tok {
            if null_tok.text.eq_ignore_ascii_case("null") {
                let written = &source[tok.start..null_tok.end];
                let remedy = if negated { "col != null" } else { "col = null" };
                return Some(ParseDiagnosis {
                    subcategory: crate::uri_registry::subcat::PARSE_IS_NULL,
                    message: format!(
                        "'{written}' is SQL, not DelightQL — there is no `is null`. \
                         `=` is the null-safe equality (IS NOT DISTINCT FROM): write \
                         `{remedy}`."
                    ),
                });
            }
        }
    }
    None
}

/// `_ (…)` — the anonymous table constructor is one token.
fn diagnose_anon_space(tokens: &[Token]) -> Option<ParseDiagnosis> {
    for pair in tokens.windows(2) {
        if pair[0].text == "_" && pair[1].text == "(" && pair[1].start > pair[0].end {
            return Some(ParseDiagnosis {
                subcategory: crate::uri_registry::subcat::PARSE_ANON_SPACE,
                message: "the anonymous table constructor is ONE token — no space \
                          between `_` and `(`: write `_(id @ 1)`, not `_ (id @ 1)`."
                    .to_string(),
            });
        }
    }
    None
}

/// Two infix operators in one ungrouped expression window — the PONY
/// rule: DelightQL has no operator precedence, so `a * b + c` has no
/// reading; every composition is parenthesized explicitly.
fn diagnose_pony(tokens: &[Token]) -> Option<ParseDiagnosis> {
    let is_op = |t: &Token| INFIX_OPS.contains(&t.text.as_str());
    let breaks = |t: &Token| WINDOW_BREAKERS.contains(&t.text.as_str());
    let operandish = |t: &Token| !is_op(t) && !breaks(t);

    for i in 1..tokens.len() {
        if !is_op(&tokens[i]) || !operandish(&tokens[i - 1]) {
            continue;
        }
        // Walk forward inside the window looking for a second operator.
        let mut j = i + 1;
        let mut saw_operand = false;
        while j < tokens.len() {
            let t = &tokens[j];
            if breaks(t) {
                break; // window ends — grouped or a different clause
            }
            if is_op(t) {
                if saw_operand && tokens.get(j + 1).is_some_and(operandish) {
                    let (a, b) = (&tokens[i].text, &t.text);
                    return Some(ParseDiagnosis {
                        subcategory: crate::uri_registry::subcat::PARSE_PONY,
                        message: format!(
                            "mixed operators `{a}` and `{b}` without grouping: \
                             DelightQL has NO operator precedence (no PEMDAS), so \
                             the expression has no reading. Parenthesize every \
                             composition: `((a {a} b) {b} c)` or `(a {a} (b {b} c))`."
                        ),
                    });
                }
                break; // consecutive ops or op-then-breaker: not the pattern
            }
            saw_operand = true;
            j += 1;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::cst::CstTree;

    fn diagnose(source: &str) -> Option<&'static str> {
        let tree = crate::pipeline::parser::parse_allow_errors_for_test(source);
        let cst = CstTree::new(&tree, source);
        diagnose_failed_parse(&cst.root(), source).map(|d| d.subcategory)
    }

    #[test]
    fn pony_fires_on_mixed_operators() {
        assert_eq!(diagnose("_(x @ 1), x * 2 + 1 = 3"), Some("pony"));
    }

    #[test]
    fn pony_silent_on_grouped_and_on_glob() {
        // fully grouped parses; sanity: even against a failed parse of
        // OTHER shape, a glob star + later single op must not fire
        assert_ne!(diagnose("users(*), x is null"), Some("pony"));
    }

    #[test]
    fn is_null_fires_and_teaches_negation() {
        assert_eq!(diagnose("_(x @ 1), x is null"), Some("is_null"));
        assert_eq!(diagnose("_(x @ 1), x is not null"), Some("is_null"));
    }

    #[test]
    fn anon_space_fires() {
        assert_eq!(diagnose("_ (x @ 1)"), Some("anon_space"));
    }

    #[test]
    fn dash_comment_fires_trailing_and_leading() {
        assert_eq!(diagnose("_(x @ 1), # < 1 -- trailing note"), Some("comment"));
        assert_eq!(diagnose("-- a comment\n_(x @ 1"), Some("comment"));
    }

    #[test]
    fn dash_comment_beats_patterns_inside_comment_text() {
        // The comment's prose contains an is_null shape and a PONY shape;
        // the comment diagnosis must win — the prose is not the query.
        assert_eq!(diagnose("_(x @ 1), # < 1 -- check x is null"), Some("comment"));
        assert_eq!(diagnose("_(x @ 1), # < 1 -- was x * 2 + 1"), Some("comment"));
    }

    #[test]
    fn sort_minus_fires_and_arithmetic_stays_silent() {
        assert_eq!(diagnose("_(x @ 1) |> #(-x)"), Some("sort_minus"));
        // minus deeper in the window is arithmetic, not descending intent
        assert_ne!(diagnose("_(x @ 1) |> #(0 - x) is"), Some("sort_minus"));
    }

    #[test]
    fn dash_comment_silent_on_gap_and_inside_string() {
        // `- -` with a gap is subtraction of a negative, not a comment.
        assert_ne!(diagnose("_(x @ 1), x - - 1 = 3"), Some("comment"));
        // `--` inside a string literal is ordinary text; the real mistake
        // here is `is null` and that diagnosis must still win.
        assert_eq!(diagnose("_(s @ \"a--b\"), s is null"), Some("is_null"));
    }

    #[test]
    fn multibyte_recovery_text_never_panics() {
        // Recovery text is user-controlled; the lexer must hold char
        // boundaries. Accented, CJK, and emoji, in error spans and gaps.
        let _ = diagnose("_(x @ 1), x * é + 1");
        let _ = diagnose("_(x @ 1), x * 日本語 + 1");
        let _ = diagnose("_(x @ 1), x * 🎉 + 1");
        let _ = diagnose("é日🎉");
        let _ = diagnose("_(é @ 1), é is null");
    }

    #[test]
    fn diagnosis_still_fires_after_multibyte_text() {
        // Multibyte text ahead of the pattern must not derail token
        // positions: the `--` pair is still adjacent, the `#(-` window
        // still matches.
        assert_eq!(diagnose("_(x @ \"café\"), # < 1 -- note"), Some("comment"));
        assert_eq!(diagnose("_(x @ \"café\") |> #(-x)"), Some("sort_minus"));
    }
}
