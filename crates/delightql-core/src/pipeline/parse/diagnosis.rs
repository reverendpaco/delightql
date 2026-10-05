// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Parse-failure diagnosis: teaching errors mined from what the author TYPED.
//!
//! Grammar-enforced rules can only ever produce grammar-grade errors
//! ("expected valid syntax near …"), so the rules users most need taught — no
//! operator precedence, no `is null`, `_(` as one token — are exactly the ones
//! the parser cannot explain. This pass runs AFTER a parse has failed and keys
//! every pattern on TOKEN PRESENCE. Never on recovery SHAPE: which nodes
//! survive an error shifts with every grammar regeneration, while the bytes
//! the author wrote do not.
//!
//! It speaks only when unambiguous — a specific wrong hint is worse than a
//! generic true one. Each diagnosis mints its own badge under `parse/` so it
//! is explainable and annotation-matchable.

use crate::diagnostic::{DelightQLError, DmlMarker, Effect, Parse, ParseAnon};
use crate::pipeline::syntax::Token;

/// Every operator the PONY rule counts at the value tier: arithmetic and
/// comparison alike. A comparison composes with arithmetic (`a / b > c`)
/// exactly as arithmetic composes with itself, and the reading is missing
/// for the same reason.
const INFIX_OPS: &[&str] = &[
    "+", "-", "*", "/", "%", "++", ">", "<", ">=", "<=", "!=", "<>",
];
/// The truth connectives. They end the value tier's window: `a > 1 and b > 2`
/// is two comparisons of one operator each. (A mixture of the two words is
/// the grammar's business — its refusal witness — not this reader's.)
const CONNECTIVE_WORDS: &[&str] = &["and", "AND", "or", "OR"];
/// Tokens that end an "expression window" — two infix operators are only a
/// PONY violation when nothing on this list separates them. The neck and the
/// goal marker end one because a head and its body, and one clause and the
/// next, are not one expression; the boundary between two top-level FORMS
/// has no token, and is drawn by attribution before the tokens reach here.
const WINDOW_BREAKERS: &[&str] = &[
    "(", ")", ",", ";", "|>", "~>", ":", "|", "{", "}", "[", "]", "->", ":-", "?-",
];

fn is_connective(token: &Token) -> bool {
    CONNECTIVE_WORDS.contains(&token.text.as_str())
}

/// The tokens a content-keyed pattern may read: the author's QUERY, with
/// every annotation's own span and the file's reader directive removed.
///
/// An annotation is not the query — its URI is a path whose slashes are not
/// division and whose segments are not columns — so a pattern that reads the
/// author's expression must not read it. The assertion body is real material,
/// but it is material the ordinary road already parses; a diagnosis that
/// spoke about it would be describing a position the message cannot name.
///
/// The utility file's header is not the query either. It selects how the bytes
/// are READ and contributes to no relation, so a pattern counting operators or
/// looking at what stands first would be counting the reader's own directive.
fn query_tokens(tokens: &[Token]) -> Vec<Token> {
    let mut kept = Vec::with_capacity(tokens.len());
    let mut inside = false;
    for token in tokens {
        if token.text == delightql_cst::QUERY_SEQUENCE_HEADER {
            continue;
        }
        if token.text.starts_with("(~~") {
            inside = true;
            continue;
        }
        if token.text == "~~)" {
            inside = false;
            continue;
        }
        if !inside {
            kept.push(token.clone());
        }
    }
    kept
}

pub(crate) fn diagnose(tokens: &[Token], source: &str) -> Option<DelightQLError> {
    // Dash-comment MUST run first: comment text is arbitrary prose, so a
    // `-- check x is null` line would otherwise feed the other patterns
    // words they would misread as the user's query.
    diagnose_dash_comment(tokens, source)
        .or_else(|| diagnose_retired_assertion_annotation(source))
        // The retired glyphs next: `==` recovers as two adjacent `=` tokens,
        // which the PONY pattern would otherwise read as two operators.
        .or_else(|| diagnose_retired_equality(&query_tokens(tokens)))
        // A malformed `$.x` recovers as loose `$`/`.` characters beside a
        // name, which the later patterns would read as a label or a path.
        // It reads inline definition blocks too: no annotation spells `$`.
        .or_else(|| diagnose_parameter_reference(tokens))
        .or_else(|| diagnose_structural_head(&query_tokens(tokens)))
        .or_else(|| diagnose_head_computes(&query_tokens(tokens)))
        .or_else(|| diagnose_comma_compound_value(&query_tokens(tokens)))
        .or_else(|| diagnose_lift_tail(&query_tokens(tokens), source))
        .or_else(|| diagnose_body_naming(&query_tokens(tokens)))
        .or_else(|| diagnose_bare_operator_guard(&query_tokens(tokens)))
        .or_else(|| diagnose_unmarked_effect_label(&query_tokens(tokens)))
        .or_else(|| diagnose_pure_head_effect_body(&query_tokens(tokens)))
        .or_else(|| diagnose_nested_session_directive(tokens))
        .or_else(|| diagnose_metadata_induction(tokens))
        .or_else(|| diagnose_bare_iteration_binder(&query_tokens(tokens)))
        .or_else(|| diagnose_qualified_pattern_member(&query_tokens(tokens)))
        .or_else(|| diagnose_path_variable(&query_tokens(tokens)))
        // The DML shapes read the author's own chain, so the annotation the
        // author wrote about it is not part of what they are reading.
        .or_else(|| diagnose_dml_marker(&query_tokens(tokens)))
        .or_else(|| diagnose_anonymous_target(&query_tokens(tokens)))
        .or_else(|| diagnose_nested_directive_position(&query_tokens(tokens)))
        .or_else(|| diagnose_sort_minus(tokens))
        .or_else(|| diagnose_is_null(tokens, source))
        .or_else(|| diagnose_anon_space(tokens))
        .or_else(|| diagnose_empty_anon(tokens))
        .or_else(|| diagnose_glob_argument(&query_tokens(tokens)))
        .or_else(|| diagnose_compound_relation_actual(&query_tokens(tokens)))
        // The PONY pattern reads an EXPRESSION, so it reads the query's own
        // tokens: an annotation URI's slashes are path separators.
        .or_else(|| diagnose_pony(&query_tokens(tokens)))
}

fn diagnose_retired_assertion_annotation(source: &str) -> Option<DelightQLError> {
    source.contains("(~~assert").then(|| DelightQLError::from(Parse::AssertionRetired {
     message: "the `(~~assert … ~~)` annotation has been removed — define a pure property rule and demand `assert!(property)(*)` on the relation being checked"
                .to_string(),
 }))
}

/// A `$.x` GONE WRONG. The reference is one glued form — the sigil and the
/// formal's name — standing where a value stands; the grammar can only say
/// "unexpected token" about a sigil left unglued, a formal marked in its own
/// head, or a reference asked to name or address a column. A bound read
/// from a bare name is the retired spelling of the same reference.
fn diagnose_parameter_reference(tokens: &[Token]) -> Option<DelightQLError> {
    let tokens: Vec<&Token> = tokens.iter().filter(|token| !token.extra).collect();
    let glued = |left: &Token, right: &Token| left.end == right.start;
    let named = |token: &Token| is_name(token) || token.text.starts_with('`');
    let refusal =
        |message: String| Some(DelightQLError::from(Parse::ParameterReference { message }));
    for (index, token) in tokens.iter().enumerate() {
        // THE SIGIL, lexed whole or recovered as its two characters.
        let after = match token.text.as_str() {
            "$." => index + 1,
            "$" if tokens
                .get(index + 1)
                .is_some_and(|dot| dot.text == "." && glued(token, dot)) =>
            {
                index + 2
            }
            "$" => {
                if let Some(name) = tokens
                    .get(index + 1)
                    .filter(|name| glued(token, name) && named(name))
                {
                    return refusal(format!(
                        "'${0}' is not a parameter reference: a scalar formal is read as \
                         `$.{0}`, the sigil `$.` glued to its name",
                        name.text
                    ));
                }
                continue;
            }
            _ => continue,
        };
        let sigil_end = tokens[after - 1];
        let Some(name) = tokens
            .get(after)
            .filter(|name| glued(sigil_end, name) && named(name))
        else {
            return refusal(
                "`$.` must be glued to the name of a scalar formal: write `$.name`, with no \
                 space between, and a name that begins with a letter or `_`"
                    .to_string(),
            );
        };
        if index > 0 && tokens[index - 1].text.eq_ignore_ascii_case("as") {
            return refusal(format!(
                "`$.{0}` reads a formal; it is a value, and `as` takes a new column name. A \
                 column may share the formal's spelling: write `as {0}`",
                name.text
            ));
        }
        if index > 0 && matches!(tokens[index - 1].text.as_str(), "-(" | "*(") {
            return refusal(format!(
                "`$.{0}` reads a formal; it is a value and addresses no column. A column is \
                 written bare",
                name.text
            ));
        }
        // A HEAD DECLARES: every token before the first neck of its clause.
        let neck = tokens[index..]
            .iter()
            .position(|next| matches!(next.text.as_str(), ":-" | ":"))
            .map(|offset| index + offset);
        let clause_start = tokens[..index]
            .iter()
            .rposition(|prior| matches!(prior.text.as_str(), ":-" | ":"))
            .map_or(0, |at| at + 1);
        if let Some(neck) = neck {
            let head = &tokens[clause_start..neck];
            let is_head = head.iter().filter(|t| t.text == ")").count() >= 1
                && tokens[neck].start > token.start
                && head.iter().all(|t| {
                    !matches!(t.text.as_str(), "|>" | "," | ";" | "@") || t.start > token.start
                });
            if is_head && tokens[clause_start..index].iter().any(|t| t.text == "(") {
                return refusal(format!(
                    "a formal is declared bare in its head — `{0}` — and read in the body as \
                     `$.{0}`",
                    name.text
                ));
            }
        }
    }
    // THE RETIRED BOUND SPELLING: `#< n` read a formal bare.
    for (index, token) in tokens.iter().enumerate() {
        if !matches!(token.text.as_str(), "#<" | "#>" | "#") {
            continue;
        }
        let at = if token.text == "#" {
            match tokens.get(index + 1) {
                Some(op) if matches!(op.text.as_str(), "<" | ">") => index + 2,
                _ => continue,
            }
        } else {
            index + 1
        };
        if let Some(name) = tokens.get(at).filter(|name| is_name(name)) {
            return refusal(format!(
                "a bound reads a formal as `$.{0}`: write `#< $.{0}`",
                name.text
            ));
        }
    }
    None
}

/// The group openers a token stream nests through. A depth reader that misses
/// one reads an interior position as a top-level one.
const OPENERS: &[&str] = &[
    "(", "_(", "?_(", "+_(", "$(", "+$(", "$$(", "-(", "*(", "+(", "#(", ":(", "%(", "^(",
];

/// The depth after each token, and the depth before it.
fn depths(tokens: &[Token]) -> Vec<i32> {
    let mut depth = 0i32;
    let mut before = Vec::with_capacity(tokens.len());
    for token in tokens {
        before.push(depth);
        if OPENERS.contains(&token.text.as_str()) {
            depth += 1;
        } else if token.text == ")" {
            depth -= 1;
        }
    }
    before
}

/// Whether the token at `index` is a `!` glued to the name before it — how
/// the marker lexes.
fn marker_at(tokens: &[Token], index: usize) -> Option<&Token> {
    let marker = tokens.get(index)?;
    if marker.text != "!" {
        return None;
    }
    let name = tokens.get(index.wrapping_sub(1))?;
    (name.end == marker.start).then_some(name)
}

/// STRUCTURAL HEAD GROUNDING IS RESERVED: `foo(T(*), {.name})(*) :- …`. The
/// brace group in a head position has no derivation, and the refusal it
/// deserves is the reservation rather than an unexpected token.
fn diagnose_structural_head(tokens: &[Token]) -> Option<DelightQLError> {
    let before = depths(tokens);
    let neck = tokens
        .iter()
        .enumerate()
        .find_map(|(index, token)| (token.text == ":-" && before[index] == 0).then_some(index))?;
    let brace = tokens[..neck]
        .iter()
        .enumerate()
        .find(|(index, token)| token.text == "{" && before[*index] > 0)?;
    let dotted = tokens
        .get(brace.0 + 1)
        .is_some_and(|token| token.text == ".");
    dotted.then(|| {
        DelightQLError::from(Parse::StructuralHead {
            message: "structural head grounding is reserved — a head parameter names a \
                  relation or a scalar, not a shape to destructure"
                .to_string(),
        })
    })
}

/// A BARE INFIX OPERATOR IN A GUARD. `f:(n | n % 2 = 0)` reads `%` as the
/// group-modulo sigil wherever a relational reading is possible, so the guard
/// has no derivation; the fix is the same parenthesization every composition
/// needs.
///
/// THE DIAGNOSIS READS EXACTLY ONE GUARD: the truth expression from its `|`
/// to the comma that starts the next parameter or the close of the group
/// it stands in — never a sibling parameter's guard. Only operators AT THE
/// GUARD'S OWN DEPTH compose: `(n | (n % 2) = 0)` has one operator at the
/// guard's level, and the `%` inside the group is that group's business.
/// A connective ends one window and opens the next, as it does at the value
/// tier: `x > 1 and x < 5` is two comparisons of one operator each. Counting
/// across any of those boundaries would blame a lawful guard for whatever
/// else the statement got wrong.
fn diagnose_bare_operator_guard(tokens: &[Token]) -> Option<DelightQLError> {
    let before = depths(tokens);
    for (index, token) in tokens.iter().enumerate() {
        if token.text != "|" || before[index] == 0 {
            continue;
        }
        let depth = before[index];
        // The guard runs to the next parameter or the close of its group.
        let end = (index + 1..tokens.len())
            .find(|at| before[*at] < depth || (before[*at] == depth && tokens[*at].text == ","))
            .unwrap_or(tokens.len());
        let mut window = 0usize;
        for at in index + 1..end {
            if before[at] != depth {
                continue;
            }
            let text = tokens[at].text.as_str();
            if is_connective(&tokens[at]) {
                window = 0;
            } else if INFIX_OPS.contains(&text) || text == "=" {
                window += 1;
            }
            if window >= 2 {
                return Some(DelightQLError::from(Parse::GuardGrouping {
                    message: "a guard composes operators and DelightQL has no precedence — \
                              parenthesize the arithmetic, e.g. `f:(n | (n % 2) = 0)`"
                        .to_string(),
                }));
            }
        }
    }
    None
}

/// AN EFFECT BINDING'S LABEL CARRIES THE MARK. `… |> temp_table!(t(*))(*) : c`
/// binds an effectful body under a pure label, which has no derivation: the
/// label asserts what its body is, and a pure label over an effect body is the
/// assertion being wrong rather than a shape the grammar forgot.
fn diagnose_unmarked_effect_label(tokens: &[Token]) -> Option<DelightQLError> {
    let before = depths(tokens);
    for (index, token) in tokens.iter().enumerate() {
        if token.text != ":" || before[index] != 0 {
            continue;
        }
        // A LABEL is `: name` with no `!` after the name.
        let Some(name) = tokens.get(index + 1) else {
            continue;
        };
        if !name
            .text
            .chars()
            .next()
            .is_some_and(|c| c.is_alphabetic() || c == '_')
        {
            continue;
        }
        if marker_at(tokens, index + 2).is_some() {
            continue;
        }
        // The body it labels is everything left of the colon back to the
        // previous top-level label or the neck.
        let opened = tokens[..index]
            .iter()
            .rposition(|token| token.text == ":-")
            .map_or(0, |at| at + 1);
        // The binding's OWN demand: a directive nested in a group belongs to
        // that group's position, and the position patterns own it.
        let demanded = (opened..index)
            .filter(|at| before[*at] == 0)
            .find_map(|at| marker_at(tokens, at));
        if let Some(demanded) = demanded {
            return Some(DelightQLError::from(Parse::EffectLabel {
                message: format!(
                    "the binding '{}' demands the directive '{}!', so its label must be \
                     '!'-marked: write '{}!'",
                    name.text, demanded.text, name.text
                ),
            }));
        }
    }
    None
}

/// A PURE HEAD OVER AN EFFECTFUL BODY. A relational rule's body is a `relex`
/// and an effect rule's is an `effrelex`, so a head without `!` whose body
/// demands a directive has no derivation — the rule the author broke is the
/// receipt algebra's R1, and the grammar can only say "unexpected token".
///
/// Keyed on what the author TYPED: a neck at group depth zero, a head left of
/// it that carries no `!` on its own subject, and a directive call to the
/// right of it.
fn diagnose_pure_head_effect_body(tokens: &[Token]) -> Option<DelightQLError> {
    let mut depth = 0i32;
    let mut neck: Option<usize> = None;
    for (index, token) in tokens.iter().enumerate() {
        match token.text.as_str() {
            "(" | "_(" | "?_(" | "+_(" | "$(" | "+$(" | "$$(" | "-(" | "*(" | "+(" | "#(" => {
                depth += 1
            }
            ")" => depth -= 1,
            ":-" if depth == 0 && neck.is_none() => neck = Some(index),
            _ => {}
        }
    }
    let neck = neck?;
    // THE SUBJECT'S MARK. A `!` anywhere left of the neck would also catch a
    // directive written inside the head's own parameters, so only the
    // subject's own position counts: the token right before the head's
    // opening group.
    let head_open = tokens[..neck]
        .iter()
        .position(|token| token.text.starts_with('(') || token.text == "_(")?;
    let subject_marked = tokens[..neck]
        .get(head_open.wrapping_sub(1))
        .zip(tokens[..neck].get(head_open.wrapping_sub(2)))
        .is_some_and(|(mark, _)| mark.text == "!");
    if subject_marked {
        return None;
    }
    // A DIRECTIVE IN THE BODY: a name immediately followed by `!`, which is
    // how the marker lexes.
    let body = &tokens[neck + 1..];
    let demanded = body.iter().enumerate().find_map(|(index, token)| {
        if token.text != "!" {
            return None;
        }
        let name = body.get(index.wrapping_sub(1))?;
        (name.end == token.start).then(|| name.text.clone())
    })?;
    let subject = tokens
        .get(head_open.wrapping_sub(1))
        .map(|token| token.text.clone())
        .unwrap_or_else(|| "this rule".to_string());
    Some(DelightQLError::from(Parse::EffectPurity {
        message: format!(
            "definition '{subject}': its head lacks '!' but its body demands the \
             directive '{demanded}!' — a rule without the effect marker must not \
             contain a directive. Declare the effect in the head: \
             '{subject}!(*) :- …'."
        ),
    }))
}

/// A session directive standing INSIDE a group. R9: a session directive is
/// legal at the REPL/CLI top level or in the liminal space, and nowhere else;
/// the grammar admits an effect call only through the effect chain's own
/// alternatives, so a directive written in a data position has no derivation
/// and the refusal it deserves has no other road to reach the author.
///
/// Keyed on GROUP DEPTH, which the tokens carry, and on the callee's declared
/// category — never on the shape recovery happened to leave behind.
fn diagnose_nested_session_directive(tokens: &[Token]) -> Option<DelightQLError> {
    let mut depth = 0i32;
    for (index, token) in tokens.iter().enumerate() {
        match token.text.as_str() {
            "(" | "_(" | "?_(" | "+_(" | "$(" | "+$(" | "$$(" | "-(" | "*(" | "+(" | "#("
            | ":(" | "%(" | "^(" => depth += 1,
            ")" => depth -= 1,
            "!" if depth > 0 => {
                // The marker lexes on its own; the name is the token before it.
                let Some(name) = tokens.get(index.wrapping_sub(1)) else {
                    continue;
                };
                if name.end != token.start {
                    continue;
                }
                if crate::pipeline::asts::effects::directive_category(&name.text)
                    != crate::pipeline::asts::effects::DirectiveCategory::Session
                {
                    continue;
                }
                return Some(DelightQLError::from(Parse::SessionPosition {
                    message: format!(
                        "{}!: session directives are legal only at the REPL/CLI top level \
                         or the liminal space — not nested in a query",
                        name.text
                    ),
                }));
            }
            _ => {}
        }
    }
    None
}

/// A NON-SESSION DIRECTIVE IN A PREDICATE POSITION. Under an effect head the
/// law admits a directive inside a predicate subquery; what is missing is its
/// lowering, so nothing derives there and the honest answer names the gap
/// rather than the shape.
///
/// Read from the author's QUERY tokens: a failed parse re-lexes an
/// annotation's own bytes, and its parens would move a depth reader that is
/// counting the author's groups.
fn diagnose_nested_directive_position(tokens: &[Token]) -> Option<DelightQLError> {
    let before = depths(tokens);
    for index in 0..tokens.len() {
        if before[index] == 0 {
            continue;
        }
        let Some(name) = marker_at(tokens, index) else {
            continue;
        };
        if crate::pipeline::asts::effects::directive_category(&name.text)
            == crate::pipeline::asts::effects::DirectiveCategory::Session
        {
            continue;
        }
        return Some(DelightQLError::from(Parse::DirectivePosition {
            message: format!(
                "{}!: predicate-position lowering is not yet supported in v0.1 — lift it \
                 out of the predicate and demand it as its own step",
                name.text
            ),
        }));
    }
    None
}

/// A call standing in a defining head: `h(count:(a)) : body`.
///
/// HEADS-law: a head position holds a name or a ground term — a head that
/// computes is not a head. The grammar cannot derive the shape, so the rule
/// the author broke is only reachable from the tokens they typed: a plain
/// head group before a top-level neck, with an application opener inside it.
fn diagnose_head_computes(tokens: &[Token]) -> Option<DelightQLError> {
    // The head is the form's first tokens: a subject name and a PLAIN group.
    // A `:(`-opened head is a CFE, whose parameters lawfully declare
    // callables (`f:()`), so only the plain opener is this shape.
    let subject_at = next_significant_index(tokens, 0)?;
    let subject = &tokens[subject_at];
    if !subject.text.chars().next().is_some_and(is_name_start) {
        return None;
    }
    let open = subject_at + 1;
    if tokens.get(open).map(|t| t.text.as_str()) != Some("(") {
        return None;
    }
    // Walk to the head group's close, watching for an application opener.
    let mut depth = 0i32;
    let mut computes: Option<&Token> = None;
    let mut close = None;
    for (index, token) in tokens.iter().enumerate().skip(open) {
        if OPENERS.contains(&token.text.as_str()) {
            if token.text == ":(" && computes.is_none() {
                computes = tokens.get(index.wrapping_sub(1));
            }
            depth += 1;
        } else if token.text == ")" {
            depth -= 1;
            if depth == 0 {
                close = Some(index);
                break;
            }
        }
    }
    let close = close?;
    let neck = next_significant(tokens, close + 1)?;
    if !matches!(neck.text.as_str(), ":" | ":-") {
        return None;
    }
    let callee = computes?;
    Some(DelightQLError::from(Parse::HeadComputes {
        message: format!(
            "a head lists names, and `{callee}:(…)` computes — a computation is not a \
             name, so a head that computes is not a head. Compute in the body and \
             label it: `{subject}(*) {neck} body |> ({callee}:(…) as n)`",
            callee = callee.text,
            subject = subject.text,
            neck = neck.text,
        ),
    }))
}

/// A comma compound written as a VALUE: `+((age > 30, city = "B") as t)`.
///
/// Comma and semicolon construct relational compounds; a value position
/// composes truths with `and`/`or`. Keyed on a parenthesized group holding a
/// comparison beside a top-level comma, closed and then named.
fn diagnose_comma_compound_value(tokens: &[Token]) -> Option<DelightQLError> {
    let comparisons = ["=", "!=", "<>", ">", "<", ">=", "<="];
    for (index, token) in tokens.iter().enumerate() {
        if token.text != "as" || index == 0 {
            continue;
        }
        if tokens[index - 1].text != ")" {
            continue;
        }
        // Walk back to the matching opener, reading the group's own depth.
        let mut depth = 0i32;
        let mut open = None;
        for at in (0..index).rev() {
            let text = tokens[at].text.as_str();
            if text == ")" {
                depth += 1;
            } else if OPENERS.contains(&text) {
                depth -= 1;
                if depth == 0 {
                    open = Some(at);
                    break;
                }
            }
        }
        let open = open?;
        if tokens[open].text != "(" {
            continue;
        }
        // At the group's own level: a comma AND a comparison — the compound.
        let mut level = 0i32;
        let mut comma = false;
        let mut compared = false;
        for token in &tokens[open + 1..index - 1] {
            let text = token.text.as_str();
            if OPENERS.contains(&text) {
                level += 1;
            } else if text == ")" {
                level -= 1;
            } else if level == 0 && text == "," {
                comma = true;
            } else if level == 0 && comparisons.contains(&text) {
                compared = true;
            }
        }
        if comma && compared {
            return Some(DelightQLError::from(Parse::Sigil {
                message: "comma and semicolon construct relational compounds, so \
                          `(a > x, b = y)` is not a value — in a value position, \
                          compose truths with `and` / `or`: `(a > x and b = y) as t`"
                    .to_string(),
            }));
        }
    }
    None
}

/// A lift tail in a ONE-group call.
///
/// `&` bounds arguments only where lifted rows follow in a two-group call
/// (`f(users(*) & 1, 2)(*)`); a one-group call's parentheses are the
/// arguments alone, and a lifted tail has no meaning there. What the author
/// reached for is read off the tail itself: bare names are a projection,
/// which belongs to the ACCESS group (`json_each(doc, path)(value, type)`);
/// anything else is rows, and the call is missing its access group
/// (`like_any(x & "a"; "b")(*)`). The remedy is the author's own call,
/// rewritten.
fn diagnose_lift_tail(tokens: &[Token], source: &str) -> Option<DelightQLError> {
    let before = depths(tokens);
    for (index, token) in tokens.iter().enumerate() {
        if token.text != "&" || before[index] == 0 {
            continue;
        }
        // `&(` is the ER-context operator, not the lift.
        if tokens
            .get(index + 1)
            .is_some_and(|next| next.text.starts_with('('))
        {
            continue;
        }
        // The enclosing group must be a CALL: its opener a plain paren glued
        // to a name.
        let level = before[index];
        let open = (0..index).rev().find(|at| before[*at] < level)?;
        if tokens[open].text != "(" {
            continue;
        }
        let callee = tokens.get(open.wrapping_sub(1))?;
        if callee.end != tokens[open].start
            || !callee.text.chars().next().is_some_and(is_name_start)
        {
            continue;
        }
        // A SECOND group after the close makes the left one an ho_part,
        // where the lift is lawful; only the one-group shape refuses here.
        // A re-lexed annotation opener is not a group: recovery may hand
        // `(~~…` back as a bare `(` glued to a `~~…` remnant.
        let close =
            (index + 1..tokens.len()).find(|at| tokens[*at].text == ")" && before[*at] == level)?;
        if let Some(at) = next_significant_index(tokens, close + 1) {
            let next = &tokens[at];
            let annotation = next.text.starts_with("(~~")
                || (next.text == "("
                    && tokens
                        .get(at + 1)
                        .is_some_and(|n| n.start == next.end && n.text.starts_with('~')));
            if next.text.starts_with('(') && !annotation {
                continue;
            }
        }
        let call = source.get(callee.start..tokens[close].end)?;
        let args = source.get(tokens[open].end..token.start)?.trim();
        let tail = source.get(token.end..tokens[close].start)?.trim();
        let names_only = tokens[index + 1..close]
            .iter()
            .filter(|token| !token.extra)
            .all(|token| token.text == "," || is_plain_name(&token.text));
        // A remedy is copied, so it is shown whole; past a line's worth the
        // arguments are elided rather than the shape.
        let short = call.chars().count() <= LIFT_TAIL_ECHO;
        let remedy = match (names_only, short) {
            (true, true) => format!(
                "a projection belongs to the ACCESS group: `{}({args})({tail})`",
                callee.text
            ),
            (true, false) => format!(
                "a projection belongs to the ACCESS group: `{}(…)({tail})`",
                callee.text
            ),
            (false, true) => format!("lifted rows need the access group after them: `{call}(*)`"),
            (false, false) => format!(
                "lifted rows need the access group after them: `{}(… & …)(*)`",
                callee.text
            ),
        };
        let shown = if short {
            call.to_string()
        } else {
            format!("{}(…)", callee.text)
        };
        return Some(DelightQLError::from(Parse::LiftTail {
            message: format!(
                "`&` bounds arguments only in a two-group call, where lifted rows \
                 follow it; `{shown}` has one group, so {remedy}"
            ),
        }));
    }
    None
}

/// A row of named values as a definition's body:
/// `f:(…) : (a as x, b as y)`.
///
/// A definition's body is ONE domain expression, and `as` names publication
/// positions — a parenthesized list of named values is a row, which a value
/// definition does not produce.
fn diagnose_body_naming(tokens: &[Token]) -> Option<DelightQLError> {
    let before = depths(tokens);
    // A top-level neck followed directly by a parenthesized group.
    let neck = tokens.iter().enumerate().find_map(|(index, token)| {
        (matches!(token.text.as_str(), ":" | ":-") && before[index] == 0).then_some(index)
    })?;
    let open = next_significant_index(tokens, neck + 1)?;
    if tokens[open].text != "(" {
        return None;
    }
    // At the body group's own level: a naming beside a comma is the row.
    let level = before[open] + 1;
    let mut named = false;
    let mut comma = false;
    for at in open + 1..tokens.len() {
        if before[at] < level {
            break;
        }
        if before[at] > level {
            continue;
        }
        match tokens[at].text.as_str() {
            "as" => named = true,
            "," => comma = true,
            _ => {}
        }
    }
    (named && comma).then(|| {
        DelightQLError::from(Parse::ValueNaming {
            message: "a definition's body is one domain expression — `as` names a \
                  publication position, and a parenthesized list of named values \
                  is a row, which a value definition does not produce. Publish the \
                  columns from the caller's projection instead"
                .to_string(),
        })
    })
}

/// `"k": ~> c:~> {…}` — a metadata group induced under a data key.
///
/// `"key": ~>` promises an interior TABLE — an array per group — while a
/// metadata group yields an interior RECORD, one object per group. The two
/// cannot be the same member, so the form has no derivation; what the author
/// wants is the plain keyed value whose value is a record.
///
/// Keyed on the three tokens in order — the key's colon, the induction, and
/// the metadata sigil — because that sequence is the form and nothing else
/// spells it.
/// `"k": ~> v` — a bare iteration binder. Iteration derives a record or a
/// tuple to destructure into; a bare name has no derivation, and the binder
/// for an array of plain values is written inside brackets: `"k": ~> [v]`.
fn diagnose_bare_iteration_binder(tokens: &[Token]) -> Option<DelightQLError> {
    for index in 0..tokens.len().saturating_sub(3) {
        let keyed = tokens[index].text.starts_with('"') && tokens[index + 1].text == ":";
        if !(keyed && tokens[index + 2].text == "~>") {
            continue;
        }
        let binder = &tokens[index + 3].text;
        if !binder.chars().next().is_some_and(is_name_start) {
            continue;
        }
        // A wrapped binder, a nesting, or a reduction name is lawful; only
        // the bare name followed by the member boundary is the refused
        // shape.
        if tokens
            .get(index + 4)
            .is_some_and(|token| matches!(token.text.as_str(), "}" | "," | ";"))
        {
            let key = &tokens[index].text;
            return Some(DelightQLError::from(Parse::IterationBinder {
                message: format!(
                    "a bare iteration binder has no derivation: `{key}: ~> {binder}` \
                     names nothing to destructure into. To bind each plain value of \
                     the array, write the binder inside brackets: `{key}: ~> [{binder}]`"
                ),
            }));
        }
    }
    None
}

/// `~= … {person.first}` — a qualified name written as a pattern member. A
/// pattern extracts values; a qualified name would assert an equality with an
/// existing column instead, and that is not what patterns do. The reach into
/// a document is the path binding, spelled with a leading dot.
fn diagnose_qualified_pattern_member(tokens: &[Token]) -> Option<DelightQLError> {
    let destructure = tokens.iter().position(|token| token.text == "~=")?;
    for index in destructure..tokens.len().saturating_sub(4) {
        let opens = matches!(tokens[index].text.as_str(), "{" | ",");
        let member = tokens[index + 1]
            .text
            .chars()
            .next()
            .is_some_and(is_name_start)
            && tokens[index + 2].text == "."
            && tokens[index + 3]
                .text
                .chars()
                .next()
                .is_some_and(is_name_start);
        if !(opens && member) {
            continue;
        }
        if tokens
            .get(index + 4)
            .is_some_and(|token| matches!(token.text.as_str(), "," | "}"))
        {
            let qualifier = &tokens[index + 1].text;
            let name = &tokens[index + 3].text;
            return Some(DelightQLError::from(Parse::PatternQualified {
                message: format!(
                    "a pattern member cannot be qualified: a pattern extracts values, \
                     and `{qualifier}.{name}` would assert an equality with an existing \
                     column instead. Reach into the document with a path binding: \
                     `.{qualifier}.{name}` publishes `{qualifier}_{name}`, and `as` renames"
                ),
            }));
        }
    }
    None
}

fn diagnose_metadata_induction(tokens: &[Token]) -> Option<DelightQLError> {
    // `"k" : ~> <key column> : ~>`. The metadata sigil admits interior
    // whitespace, so it reaches the recovery lexer as its two halves and is
    // matched here as the pair it is spelled with. The key column is a
    // reference, so it may carry a qualifier.
    for index in 0..tokens.len().saturating_sub(4) {
        let induced = tokens[index].text.starts_with('"')
            && tokens[index + 1].text == ":"
            && tokens[index + 2].text == "~>";
        if !induced {
            continue;
        }
        let mut cursor = index + 3;
        let column_start = cursor;
        while tokens.get(cursor).is_some_and(|token| {
            token.text == "." || token.text.chars().next().is_some_and(is_name_start)
        }) {
            cursor += 1;
        }
        if cursor == column_start {
            continue;
        }
        // The sigil survives whole where the lexer reached it and as its two
        // halves where recovery re-lexed the span; both are the same spelling.
        let whole = tokens
            .get(cursor)
            .is_some_and(|token| token.text.replace(char::is_whitespace, "") == ":~>");
        let halves = tokens.get(cursor).is_some_and(|token| token.text == ":")
            && tokens
                .get(cursor + 1)
                .is_some_and(|token| token.text == "~>");
        if !whole && !halves {
            continue;
        }
        let key = &tokens[index].text;
        let column: String = tokens[column_start..cursor]
            .iter()
            .map(|token| token.text.as_str())
            .collect();
        return Some(DelightQLError::from(Parse::MetadataInduction {
            message: format!(
                "a metadata group is one object per group, and `{key}: ~>` induces a \
                 table — a metadata group stands under a fixed key by its own \
                 spelling: `{key}: ~> {column}:~> {{…}}` in a PATTERN, and \
                 `{key}: {column}:~> {{…}}` in a CONSTRUCTION"
            ),
        }));
    }
    None
}

/// How long a call a lift-tail refusal echoes whole.
const LIFT_TAIL_ECHO: usize = 72;

/// A bare identifier: what a projection list spells, and a row of values
/// does not.
fn is_plain_name(text: &str) -> bool {
    let mut chars = text.chars();
    chars.next().is_some_and(|c| c.is_alphabetic() || c == '_')
        && chars.all(|c| c.is_alphanumeric() || c == '_')
}

fn is_name_start(c: char) -> bool {
    c.is_alphabetic() || c == '_' || c == '`'
}

/// `x:{p}`, `x:{"$.a"}`, `x:[1]` — the accessor reached for something that is
/// not the one literal path it takes.
///
/// THERE IS ONE ACCESSOR DOOR and it takes exactly ONE path, spelled with its
/// steps. A path is SPEC, not a value: it never evaluates alone and nothing
/// produces one at runtime, so a bare name inside the braces can never be fed —
/// and a bare name is also what a path missing its leading dot looks like, so
/// one teaching answers both. `"$…"` is the TARGET's path sub-language, which
/// stays with the target. `:[n]` says the same thing as `:{.n}` with a shape
/// that reads as a type: an accessor READS, so it takes the one path spelling.
///
/// Keyed on ADJACENCY, which is what makes `:{` an accessor rather than a key's
/// colon before a record: `x:{p}` runs unbroken from the name through the
/// close, while `"k": {p}` is a string, a colon, and a separate record.
fn diagnose_path_variable(tokens: &[Token]) -> Option<DelightQLError> {
    for index in 1..tokens.len().saturating_sub(2) {
        // The accessor's open reaches recovery whole where the lexer had it and
        // as two characters where the span was re-lexed; both are one opening.
        let (payload, close, bracket) = match tokens[index].text.as_str() {
            ":{" => (index + 1, index + 2, false),
            ":[" => (index + 1, index + 2, true),
            ":" if tokens[index].end == tokens[index + 1].start
                && matches!(tokens[index + 1].text.as_str(), "{" | "[") =>
            {
                (index + 2, index + 3, tokens[index + 1].text == "[")
            }
            _ => continue,
        };
        // The value it reads is glued to the accessor. Without this a key's
        // colon before a record member would read as an accessor.
        let subject = &tokens[index - 1];
        if subject.end != tokens[index].start
            || !subject.text.chars().next().is_some_and(is_name_start)
        {
            continue;
        }
        let Some(written) = tokens.get(payload) else {
            continue;
        };
        let closer = if bracket { "]" } else { "}" };
        if tokens.get(close).map(|token| token.text.as_str()) != Some(closer) {
            continue;
        }
        // A bare dot is the whole document, and the whole document is the
        // column's own value: the path grammar reaches INSIDE (`('.' key)+`),
        // so there is no zero-step path to write.
        if !bracket && written.text == "." && tokens[close].text == "}" {
            let subject = &subject.text;
            return Some(DelightQLError::from(Parse::PathVariable {
                message: format!(
                    "`{subject}:{{.}}` reaches for the whole document, and the whole \
                     document is the column itself — write `{subject}`; a path reaches \
                     inside: `{subject}:{{.key}}`"
                ),
            }));
        }
        // A leading dot inside braces is a path being SPELLED, however badly.
        // This teaching is about the accessor's DOOR and what may stand behind
        // it; a malformed path keeps its own complaint.
        if !bracket && written.text.starts_with('.') {
            continue;
        }
        let subject = &subject.text;
        let advice = if bracket {
            format!(
                "there is one accessor door and it is `:{{…}}` — reach with \
                 `{subject}:{{.{}}}`",
                written.text
            )
        } else if written.text.starts_with('"') {
            "a target's path spelling stays with the target — reach with `x:{.a.b}`".to_string()
        } else {
            format!(
                "a path is spelled with its steps — `{subject}:{{.{}}}` — and a path \
                 is spec, not a value, so a name standing here can never be fed one",
                written.text
            )
        };
        return Some(DelightQLError::from(Parse::PathVariable {
            message: format!(
                "the json accessor takes exactly one LITERAL path, and `{}` is not \
                 one — {advice}",
                written.text
            ),
        }));
    }
    None
}

/// The mutation marker in a shape the grammar cannot derive.
///
/// `!!` marks ONE relation — the one being written — and it marks the chain's
/// HEAD, because a mutation source is that target with its restrictions
/// hanging off it. A second mark and a mark on a member the chain merely
/// joins both have no derivation, so the refusal they deserve has no other
/// road to the author. Counted over the author's own tokens: `!!` is one
/// token and nothing else spells it.
fn diagnose_dml_marker(tokens: &[Token]) -> Option<DelightQLError> {
    // The marker is GLUED to the name it marks — that is what makes it a
    // marker rather than two characters — so a `!!` standing on its own,
    // inside a blob's payload or wherever else recovery re-lexed, marks
    // nothing.
    let marks: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(index, token)| {
            token.text == "!!"
                && index
                    .checked_sub(1)
                    .and_then(|before| tokens.get(before))
                    .is_some_and(|before| {
                        before.end == token.start
                            && before
                                .text
                                .ends_with(|c: char| c.is_alphanumeric() || c == '_' || c == '`')
                    })
        })
        .map(|(index, _)| index)
        .collect();
    if marks.len() > 1 {
        return Some(DelightQLError::from(DmlMarker::Multiple {
    message: format!(
                "{} relations carry the mutation marker; a statement writes to ONE —                  mark the relation being written and join the rest unmarked",
                marks.len()
            ),
}));
    }
    // ONE mark, and something joined before it: the marked relation is not
    // the one the chain is built from.
    let mark = *marks.first()?;
    let mut depth = 0i32;
    let joined_before = tokens[..mark]
        .iter()
        .any(|token| match token.text.as_str() {
            "," => depth == 0,
            ")" => {
                depth -= 1;
                false
            }
            text if text.ends_with('(') => {
                depth += 1;
                false
            }
            _ => false,
        });
    joined_before.then(|| {
        DelightQLError::from(DmlMarker::Mismatch {
            message: "the mutation marker is on a relation the chain JOINS, not the one it \
                  is built from — mark the source the statement writes to and put it first"
                .to_string(),
        })
    })
}

/// An anonymous table where a target designator belongs.
///
/// A target NAMES where the effect lands, and an anonymous table has no name
/// to be — so `delete!(_(*))(*)` designates nothing. The grammar has no
/// derivation for it; the refusal it deserves is the designator's own.
fn diagnose_anonymous_target(tokens: &[Token]) -> Option<DelightQLError> {
    const TERMINALS: &[&str] = &["insert", "update", "delete"];
    tokens.windows(4).find_map(|window| {
        let terminal = TERMINALS.contains(&window[0].text.as_str())
            && window[1].text == "!"
            && window[2].text == "("
            && window[3].text.starts_with("_(");
        terminal.then(|| {
            DelightQLError::from(Effect::DmlTargetDesignator {
                message: format!(
                    "{}!'s target is a whole-table DESIGNATOR — `name(*)`, optionally \
                 namespace-qualified — naming where to write; an anonymous table names \
                 nothing and cannot be one",
                    window[0].text
                ),
            })
        })
    })
}

/// `--` — SQL's line comment, which DelightQL does not have: it lexes as
/// two `-` operators and breaks the parse. Keyed on an ADJACENT minus
/// pair (`a - -b` has a gap and stays silent); a pair behind an odd
/// number of `"` is inside a string literal and stays silent too.
fn diagnose_dash_comment(tokens: &[Token], source: &str) -> Option<DelightQLError> {
    for pair in tokens.windows(2) {
        if pair[0].text == "-" && pair[1].text == "-" && pair[1].start == pair[0].end {
            let quotes_before = source[..pair[0].start].matches('"').count();
            if quotes_before % 2 == 1 {
                continue; // inside a string literal — ordinary text
            }
            return Some(DelightQLError::from(Parse::Comment {
                message: "`--` is not a comment in DelightQL — it lexes as two `-` \
                          operators. Line comments are `//`. (If subtraction of a \
                          negative was meant, group it: `a - (-b)`.)"
                    .to_string(),
            }));
        }
    }
    None
}

/// `#(-col)` — minus-prefix descending sort, which DelightQL does not
/// have; the spelling is `#(col desc)`. Keyed on `-` directly after the
/// sort sigil's `(`; a minus deeper in the window (`#(0 - col)`) is
/// arithmetic and stays silent.
fn diagnose_sort_minus(tokens: &[Token]) -> Option<DelightQLError> {
    // `#(` is ONE token — the ordering sigil's own opener.
    for w in tokens.windows(2) {
        if w[0].text == "#(" && w[1].text == "-" {
            return Some(DelightQLError::from(Parse::SortMinus {
                message: "`#(-col)` is not descending sort — the spelling is \
                          `#(col desc)`, per key: `#(a desc, b)`. (Unary minus \
                          as arithmetic needs grouping: `#((0 - col))`.)"
                    .to_string(),
            }));
        }
    }
    None
}

/// `==` / `!==` — the retired spellings of the target's own equality. Neither
/// is a token: `==` recovers as two byte-adjacent `=` leaves and `!==` as `!=`
/// glued to `=`. The diagnosis names both roads an author may have meant —
/// DelightQL's null-safe operator, or the explicit prelude predicate — and
/// derives nothing: no grammar production, normalization arm or lowering
/// admits either glyph.
fn diagnose_retired_equality(tokens: &[Token]) -> Option<DelightQLError> {
    for pair in tokens.windows(2) {
        let glued = pair[1].start == pair[0].end && pair[1].text == "=";
        if !glued {
            continue;
        }
        let (glyph, delightql, predicate) = match pair[0].text.as_str() {
            "=" => ("==", "=", "equality"),
            "!=" => ("!==", "!=", "inequality"),
            _ => continue,
        };
        let sigma = if glyph == "==" { "sql_eq" } else { "sql_ne" };
        return Some(DelightQLError::from(Parse::RetiredOperator {
            message: format!(
                "`{glyph}` is no longer DelightQL syntax; use `{delightql}` for \
                 DelightQL {predicate} or `+{sigma}(l, r)` for the target SQL \
                 operation"
            ),
        }));
    }
    None
}

/// `col is null` / `col is not null` — SQL spelling with no DelightQL
/// counterpart; `=` is the null-safe equality.
fn diagnose_is_null(tokens: &[Token], source: &str) -> Option<DelightQLError> {
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
                return Some(DelightQLError::from(Parse::IsNull {
                    message: format!(
                        "'{written}' is SQL, not DelightQL — there is no `is null`. \
                         `=` is the null-safe equality (IS NOT DISTINCT FROM): write \
                         `{remedy}`."
                    ),
                }));
            }
        }
    }
    None
}

/// `_ (…)` — the anonymous table constructor is one token.
fn diagnose_anon_space(tokens: &[Token]) -> Option<DelightQLError> {
    for pair in tokens.windows(2) {
        if pair[0].text == "_" && pair[1].text == "(" && pair[1].start > pair[0].end {
            return Some(DelightQLError::from(Parse::AnonSpace {
                message: "the anonymous table constructor is ONE token — no space \
                          between `_` and `(`: write `_(id @ 1)`, not `_ (id @ 1)`."
                    .to_string(),
            }));
        }
    }
    None
}

/// `_()` — there is no empty anonymous table. The grammar wants a row
/// inside the parens and reports a MISSING identifier, which explains
/// nothing; the ruled reading is that the form names no relation at all.
fn diagnose_empty_anon(tokens: &[Token]) -> Option<DelightQLError> {
    // `_(` lexes as one token — the constructor's own opener.
    for pair in tokens.windows(2) {
        if pair[0].text == "_(" && pair[1].text == ")" && pair[1].start == pair[0].end {
            return Some(DelightQLError::from(ParseAnon::Empty {
                message: "there is no empty anonymous table: `_()` names no relation. \
                          The union identity is the empty relation of the matching schema; \
                          its typed spelling (`_(cols @)`) is reserved and not yet available"
                    .to_string(),
            }));
        }
    }
    None
}

/// The next token that carries meaning, stepping over what the grammar
/// declares EXTRA between any two tokens.
///
/// The classification is the GRAMMAR'S: a token carries whether it lies
/// inside an extra, decided in `delightql-cst` against `EXTRA_KINDS`, which
/// the typed-CST generator emits from the same `extras` list the parser was
/// built from. Adding one there reaches this reader without anyone editing
/// it, which a table of spellings here could not promise.
fn next_significant(tokens: &[Token], from: usize) -> Option<&Token> {
    tokens.get(from..)?.iter().find(|token| !token.extra)
}

/// The same step, answered as an index into the same slice.
fn next_significant_index(tokens: &[Token], from: usize) -> Option<usize> {
    (from..tokens.len()).find(|at| !tokens[*at].extra)
}

/// `f(*)(…)` — a bare glob standing where a higher-order argument stands.
///
/// With one group, `f(*)` is ordinary access. A second group makes the left
/// one an `ho_part`, and `*` is not an argument there: it names no relation,
/// so there is nothing for the callee's relation parameter to bind.
/// WHITESPACE IS NOT A DISTINCTION HERE. The two-group shape is grammatical,
/// so `f(*) (*)` and `f(*)\n(*)` are the same query as `f(*)(*)` and must
/// reach the same teaching — a byte-adjacency test would give one of them a
/// worse error for a blank.
fn diagnose_glob_argument(tokens: &[Token]) -> Option<DelightQLError> {
    for (index, window) in tokens.windows(3).enumerate() {
        let [open, glob, close] = window else {
            continue;
        };
        if open.text != "(" || glob.text != "*" || close.text != ")" {
            continue;
        }
        if next_significant(tokens, index + 3).is_some_and(|next| next.text == "(") {
            return Some(DelightQLError::from(Parse::GlobArgument {
                message: "a bare `*` is not a higher-order argument: with a second \
                          group the left one supplies parameters, and `*` names no \
                          relation for one to bind. Supply the relation itself — \
                          `f(users(*))(*)` — or land it with `@`. With ONE group, \
                          `f(*)` is ordinary access."
                    .to_string(),
            }));
        }
    }
    None
}

/// The connectives that compose RELATIONS and can stand nowhere inside a
/// higher-order argument list: a set operator or a pipe at the list's own
/// depth. `;` and `-` are not read — one is the lifted-row separator and
/// the other arithmetic there, so neither is unambiguous.
const RELATION_CONNECTIVES: &[&str] = &["|;|", "||", "|>"];

/// `f(a(*) |;| b(*))(*)` — a compound relation expression where a
/// higher-order argument stands.
///
/// The mixed argument list embeds no relation grammar: an argument is one
/// closed relation value, and a set expression, pipeline, or join has no
/// derivation there. The shape is a name's group carrying a relation
/// connective at the group's own depth, and the group is an ARGUMENT LIST
/// on one of two proofs: it opens with a relation form (`name(` or `_(`),
/// which no interior can open with, or a second group follows it.
/// Recovery may leave the second group outside the form it attributes the
/// failure to, so the first proof does not depend on the tail being read.
fn diagnose_compound_relation_actual(tokens: &[Token]) -> Option<DelightQLError> {
    let before = depths(tokens);
    for (open, window) in tokens.windows(2).enumerate() {
        let [name, paren] = window else {
            continue;
        };
        if paren.text != "(" || !is_name(name) || name.end != paren.start {
            continue;
        }
        let inside = before[open + 1] + 1;
        let close = (open + 2..tokens.len())
            .find(|at| before[*at] == inside && tokens[*at].text == ")")
            .unwrap_or(tokens.len());
        let compound = (open + 2..close).any(|at| {
            before[at] == inside && RELATION_CONNECTIVES.contains(&tokens[at].text.as_str())
        });
        if !compound {
            continue;
        }
        let opens_with_a_relation = match (tokens.get(open + 2), tokens.get(open + 3)) {
            (Some(first), _) if first.text == "_(" => true,
            (Some(first), Some(second)) => {
                is_name(first) && second.text == "(" && first.end == second.start
            }
            _ => false,
        };
        let second_group = close < tokens.len()
            && next_significant(tokens, close + 1).is_some_and(|next| next.text == "(");
        if opens_with_a_relation || second_group {
            return Some(DelightQLError::from(Parse::HoRelationActual {
                message: format!(
                    "a higher-order argument is one closed relation value: a set \
                     expression, pipeline, or join has no derivation inside `{}(…)`. \
                     Bind the relation first — `… : name` — and pass the whole named \
                     access, `{}(name(*))(*)`.",
                    name.text, name.text
                ),
            }));
        }
    }
    None
}

/// An identifier-shaped token: what can name a callee.
fn is_name(token: &Token) -> bool {
    let mut chars = token.text.chars();
    chars
        .next()
        .is_some_and(|first| first.is_alphabetic() || first == '_')
        && chars.all(|c| c.is_alphanumeric() || c == '_')
        && token.text != "_"
}

/// The PONY rule at the value tier: DelightQL has no operator precedence,
/// so `a * b + c` has no reading; every composition is parenthesized
/// explicitly. The connective tier has no token reader: a run mixing `and`
/// with `or` is recognized by the grammar as its refusal witness wherever a
/// truth stands, and normalization refuses it with this same identity.
fn diagnose_pony(tokens: &[Token]) -> Option<DelightQLError> {
    diagnose_mixed_operators(tokens)
}

/// Two infix operators in one ungrouped expression window.
///
/// The window is read by depth exactly as the connective tier's is: a group
/// opened inside it — an argument row, an anonymous table, a parenthesized
/// operand — is ONE operand of the window, and its own tokens are a window of
/// their own. So `1 + min:(iter / 7, 4)` composes nothing, while
/// `(a + b) * c + d` does.
fn diagnose_mixed_operators(tokens: &[Token]) -> Option<DelightQLError> {
    #[derive(Clone, Copy, Default)]
    struct Window {
        /// The first binary operator of the window, once an operand precedes
        /// one.
        first: Option<usize>,
        /// Whether an operand has followed `first`.
        operand_after_first: bool,
        /// Whether the last item at this depth was an operand.
        prev_operand: bool,
    }
    let is_op = |t: &Token| INFIX_OPS.contains(&t.text.as_str());
    let breaks = |t: &Token| WINDOW_BREAKERS.contains(&t.text.as_str()) || is_connective(t);
    let opens = |t: &Token| OPENERS.contains(&t.text.as_str());
    // What may stand after a binary operator: an operand token or a group.
    let operand_follows = |index: usize| {
        tokens
            .get(index)
            .is_some_and(|t| opens(t) || (!is_op(t) && !breaks(t)))
    };
    let mark_operand = |window: &mut Window| {
        window.prev_operand = true;
        if window.first.is_some() {
            window.operand_after_first = true;
        }
    };

    let before = depths(tokens);
    let mut windows: Vec<Window> = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        let Ok(depth) = usize::try_from(before[index]) else {
            windows.clear();
            continue;
        };
        if token.text == ")" {
            // The group closes: its interior window ends, and the group is
            // one operand of the window it stood in.
            windows.truncate(depth);
            if let Some(outer) = depth.checked_sub(1).and_then(|d| windows.get_mut(d)) {
                mark_operand(outer);
            }
            continue;
        }
        if windows.len() <= depth {
            windows.resize(depth + 1, Window::default());
        }
        if opens(token) {
            windows.truncate(depth + 1);
            continue;
        }
        if breaks(token) {
            windows[depth] = Window::default();
            continue;
        }
        let window = &mut windows[depth];
        if is_op(token) {
            if let Some(first) = window.first {
                if window.operand_after_first && operand_follows(index + 1) {
                    let (a, b) = (&tokens[first].text, &token.text);
                    return Some(DelightQLError::from(Parse::Pony {
                        message: format!(
                            "mixed operators `{a}` and `{b}` without grouping: \
                             DelightQL has NO operator precedence (no PEMDAS), so \
                             the expression has no reading. Parenthesize every \
                             composition: `((a {a} b) {b} c)` or `(a {a} (b {b} c))`."
                        ),
                    }));
                }
            }
            // A binary operator follows an operand; a leading or repeated
            // operator is a sign, not a composition.
            window.first = window.prev_operand.then_some(index);
            window.operand_after_first = false;
            window.prev_operand = false;
            continue;
        }
        mark_operand(window);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The diagnosed identity's hierarchy under `parse/` (or whole, for a
    /// teaching that names another family's identity).
    fn subcategory(source: &str) -> Option<String> {
        let mut parser = crate::pipeline::syntax::Parser::new();
        let tree = parser.parse_prompt(source);
        diagnose(&tree.tokens(), tree.source()).map(|d| {
            let hierarchy = d.id().hierarchy();
            hierarchy
                .strip_prefix("parse/")
                .unwrap_or(&hierarchy)
                .to_string()
        })
    }

    #[test]
    fn pony_fires_on_mixed_operators() {
        assert_eq!(
            subcategory("_(x @ 1), x * 2 + 1 = 3").as_deref(),
            Some("pony")
        );
    }

    #[test]
    fn pony_silent_on_grouped_and_on_glob() {
        assert_ne!(subcategory("users(*), x is null").as_deref(), Some("pony"));
    }

    /// A GROUP IS ONE OPERAND OF ITS WINDOW. An argument row opened inside an
    /// expression is its own window, so `1 + min:(iter / 7, 4)` composes
    /// nothing; and a parenthesized operand counts as the operand before the
    /// next operator, so `(a + b) * c + d` is the mixture it looks like.
    #[test]
    fn pony_reads_operator_windows_by_depth() {
        assert_ne!(
            subcategory("_(x @ 1), y = 1 + min:(x / 7, 4)").as_deref(),
            Some("pony")
        );
        assert_ne!(
            subcategory("_(x @ 1), y = (x + 1) * 2").as_deref(),
            Some("pony")
        );
        assert_eq!(
            subcategory("_(x @ 1), y = (x + 1) * 2 + 3").as_deref(),
            Some("pony")
        );
        assert_eq!(
            subcategory("_(x @ 1), y = 1 + min:(x / 7 * 2, 4)").as_deref(),
            Some("pony")
        );
    }

    /// A connective ends the VALUE tier's window: a comparison on each side
    /// of `and` is not two operators composed.
    #[test]
    fn pony_silent_on_one_comparison_per_conjunct() {
        assert_ne!(
            subcategory("users(*), a > 1 and b < 2").as_deref(),
            Some("pony")
        );
        assert_eq!(
            subcategory("users(*), a + 1 > 2 and b < 2").as_deref(),
            Some("pony")
        );
    }

    /// The retired glyphs are diagnosed from token ADJACENCY, so a spaced
    /// `= =` (two operators, a PONY shape) and the living `=` / `!=` are not
    /// mistaken for them.
    #[test]
    fn retired_equality_glyphs_teach_both_roads() {
        assert_eq!(
            subcategory("users(*), a == 1").as_deref(),
            Some("retired_operator")
        );
        assert_eq!(
            subcategory("users(*), a !== 1").as_deref(),
            Some("retired_operator")
        );
        assert_eq!(subcategory("users(*), a = 1").as_deref(), None);
        assert_eq!(subcategory("users(*), a != 1").as_deref(), None);
        assert_eq!(subcategory("users(*), +sql_eq(a, 1)").as_deref(), None);
        let mut parser = crate::pipeline::syntax::Parser::new();
        let tree = parser.parse_prompt("users(*), a == 1");
        let found = diagnose(&tree.tokens(), tree.source()).expect("diagnosed");
        assert!(found.to_string().contains("`=` for DelightQL equality"));
        assert!(found.to_string().contains("`+sql_eq(l, r)`"));
        let tree = parser.parse_prompt("users(*), a !== 1");
        let found = diagnose(&tree.tokens(), tree.source()).expect("diagnosed");
        assert!(found.to_string().contains("`!=` for DelightQL inequality"));
        assert!(found.to_string().contains("`+sql_ne(l, r)`"));
    }

    /// The three bare-glob spellings, told apart by how many groups follow
    /// the name rather than by what stands inside one.
    #[test]
    fn glob_argument_fires_only_with_a_second_group() {
        assert_eq!(subcategory("users(*)(*)").as_deref(), Some("glob_argument"));
        assert_eq!(
            subcategory("users(*)(id)").as_deref(),
            Some("glob_argument")
        );
    }

    /// A compound relation expression in a higher-order argument list: read
    /// off the group opening with a relation form, so the teaching stands
    /// even when recovery attributes the failure to a form that ends before
    /// the second group.
    #[test]
    fn a_compound_relation_actual_teaches_binding_first() {
        assert_eq!(
            subcategory("f(_(x, y @ 1, 10) |;| _(x, y @ 2, 20))(*)"),
            Some("ho/relation_actual".to_string())
        );
        assert_eq!(
            subcategory("f(a(*) || b(*))(*)"),
            Some("ho/relation_actual".to_string())
        );
        assert_eq!(
            subcategory("f(a(*) |> (x))(*)"),
            Some("ho/relation_actual".to_string())
        );
        // An interior's own continuation is not an argument list.
        assert_ne!(
            subcategory("f(, x > 1 |;| g(*))"),
            Some("ho/relation_actual".to_string())
        );
        // Two arguments are two closed values.
        assert_ne!(
            subcategory("f(a(*), b(*))(*)"),
            Some("ho/relation_actual".to_string())
        );
        // ONE group is access, and access admits the glob.
        assert_eq!(subcategory("users(*)").as_deref(), None);
        // A relation supplied AS a relation is not this shape.
        assert_ne!(
            subcategory("f(users(*))(*)"),
            Some("glob_argument".to_string())
        );
    }

    /// One authored spelling per kind the grammar declares EXTRA. The kinds
    /// come from `EXTRA_KINDS`, so a new extra reaches this table as a
    /// FAILURE rather than as silently uncovered ground.
    const EXTRA_SPELLINGS: &[(&str, &[&str])] = &[
        ("comment", &[" // a note\n"]),
        ("smart_comment", &[" (/* a note */) "]),
    ];

    /// The shape is grammatical, so nothing the grammar declares EXTRA
    /// between the two groups may change which teaching the author gets.
    #[test]
    fn glob_argument_reads_through_every_extra() {
        let covered: Vec<&str> = EXTRA_SPELLINGS.iter().map(|(kind, _)| *kind).collect();
        let mut uncovered: Vec<&str> = delightql_cst::EXTRA_KINDS
            .iter()
            .copied()
            .filter(|kind| !covered.contains(kind))
            .collect();
        uncovered.sort_unstable();
        assert!(
            uncovered.is_empty(),
            "the grammar declares extras this test does not spell: {uncovered:?}"
        );

        // Whitespace is an extra that produces no node, so it is spelled here
        // rather than reached by kind.
        for between in [" ", "  \t ", "\n", "\n\n  "] {
            assert_eq!(
                subcategory(&format!("users(*){between}(*)")),
                Some("glob_argument".to_string()),
                "whitespace changed the teaching: {between:?}"
            );
        }
        for (kind, spellings) in EXTRA_SPELLINGS {
            for between in *spellings {
                assert_eq!(
                    subcategory(&format!("users(*){between}(*)")),
                    Some("glob_argument".to_string()),
                    "the {kind} extra changed the teaching: {between:?}"
                );
            }
        }
    }

    /// A NON-EXTRA IS NOT STEPPED OVER, and an extra the author never closed
    /// fails closed: recovery gives its bytes no node, so nothing classifies
    /// them as an extra and the reader stops where they begin.
    #[test]
    fn only_extras_are_stepped_over() {
        // A separator is not an extra: it makes these different queries, and
        // neither is the two-group shape.
        assert_ne!(
            subcategory("users(*), (id)").as_deref(),
            Some("glob_argument")
        );
        assert_ne!(
            subcategory("users(*) |> (id)").as_deref(),
            Some("glob_argument")
        );
        // An unterminated delimited extra runs to the end of the text, so no
        // second group stands after it to find.
        assert_ne!(
            subcategory("users(*) (/* unclosed (*)"),
            Some("glob_argument".to_string())
        );
        assert_ne!(
            subcategory("users(*) (/! unclosed (*)"),
            Some("glob_argument".to_string())
        );
    }

    /// A HEAD DOES NOT COMPUTE (heads-law): the refusal names the callee and
    /// says a computation is not a name.
    #[test]
    fn head_computes_fires_on_a_call_in_a_head() {
        assert_eq!(
            subcategory("h(count:(a)) : _(a @ 1)"),
            Some("head_computes".to_string())
        );
        // A CFE head opens with `:(` and lawfully declares callables.
        assert_ne!(
            subcategory("transform_both:(f:(), col1, col2) : (col1 >> f:() as x, col2)"),
            Some("head_computes".to_string())
        );
        // A plain listed head is not this shape, whatever else failed.
        assert_ne!(
            subcategory("h(a, b) : _(a @ 1"),
            Some("head_computes".to_string())
        );
    }

    /// Comma constructs relational compounds; a value position composes with
    /// `and`/`or`.
    #[test]
    fn comma_compound_value_fires_when_the_compound_is_named() {
        assert_eq!(
            subcategory(r#"people(*) |> +((age > 30, city = "Boston") as t)"#),
            Some("sigil".to_string())
        );
        // A single comparison named is not the compound.
        assert_ne!(
            subcategory("people(*) |> +((age > 30) as t"),
            Some("sigil".to_string())
        );
    }

    /// The one-group lift tail refuses toward the access group; a two-group
    /// call keeps the lawful lift.
    #[test]
    fn lift_tail_fires_only_with_one_group() {
        assert_eq!(
            subcategory(r#"json_each("[7,8]", "$" & value, type)"#).as_deref(),
            Some("lift_tail")
        );
        assert_eq!(
            subcategory(r#"ls(*), +like_any(ls.ns & "sys::%";"std::%")"#).as_deref(),
            Some("lift_tail")
        );
        // A second group makes the left one an ho_part, where `&` is lawful;
        // whatever else fails, it is not this teaching.
        assert_ne!(
            subcategory(r#"args(1, 100; 2, 200 & 1, "x"; 2, "y")(*)"#).as_deref(),
            Some("lift_tail")
        );
    }

    /// The remedy is read off the tail and spelled with the author's own
    /// call: bare names move into the access group; rows keep their place
    /// and gain the access group after them.
    #[test]
    fn lift_tail_rewrites_the_authors_call() {
        let message = |source: &str| {
            let tree = crate::pipeline::syntax::Parser::new().parse_prompt(source);
            diagnose(&tree.tokens(), tree.source())
                .map(|d| d.to_string())
                .unwrap_or_default()
        };
        let projection = message(r#"json_each("[7,8]", "$" & value, type)"#);
        assert!(
            projection.contains(r#"`json_each("[7,8]", "$")(value, type)`"#),
            "{projection}"
        );
        let rows = message(r#"ls(*), +like_any(ls.ns & "sys::%";"std::%")"#);
        assert!(
            rows.contains(r#"`like_any(ls.ns & "sys::%";"std::%")(*)`"#),
            "{rows}"
        );
        let long = message(&format!(
            "t(*), +f(t.x & {})",
            vec!["\"pattern-number-one\""; 6].join("; ")
        ));
        assert!(long.contains("`f(… & …)(*)`"), "{long}");
    }

    /// A definition's body is one value; a row of named values refuses.
    #[test]
    fn body_naming_fires_on_a_named_row_body() {
        assert_eq!(
            subcategory("t:(f:(), a, b) : (a >> f:() as x, b >> f:() as y)"),
            Some("value_naming".to_string())
        );
    }

    /// The whole document is the column itself; a zero-step path refuses
    /// toward the bare column.
    #[test]
    fn whole_document_path_teaches_the_bare_column() {
        assert_eq!(
            subcategory(r#"_(a @ 1) |> ({"a": a:{.}})"#).as_deref(),
            Some("path_variable")
        );
    }

    #[test]
    fn is_null_fires_and_teaches_negation() {
        assert_eq!(
            subcategory("_(x @ 1), x is null").as_deref(),
            Some("is_null")
        );
        assert_eq!(
            subcategory("_(x @ 1), x is not null").as_deref(),
            Some("is_null")
        );
    }

    #[test]
    fn anon_space_fires() {
        assert_eq!(subcategory("_ (x @ 1)").as_deref(), Some("anon_space"));
    }

    #[test]
    fn empty_anon_fires_and_leaves_inhabited_ones_alone() {
        assert_eq!(subcategory("_(1) |;| _()").as_deref(), Some("anon/empty"));
        assert_eq!(subcategory("_()").as_deref(), Some("anon/empty"));
        // A row makes it a relation; whatever else fails here, it is not this.
        assert_ne!(
            subcategory("_(x @ 1) |;| _(x @"),
            Some("anon/empty".to_string())
        );
    }

    #[test]
    fn dash_comment_fires_trailing_and_leading() {
        assert_eq!(
            subcategory("_(x @ 1), # < 1 -- trailing note").as_deref(),
            Some("comment")
        );
        assert_eq!(
            subcategory("-- a comment\n_(x @ 1"),
            Some("comment".to_string())
        );
    }

    #[test]
    fn retired_assertion_annotation_teaches_the_effect_form() {
        assert_eq!(
            subcategory("_(x @ 1) (~~assert |> exists(*) ~~)"),
            Some("assertion/retired".to_string())
        );
    }

    #[test]
    fn dash_comment_beats_patterns_inside_comment_text() {
        // The comment's prose contains an is_null shape and a PONY shape;
        // the comment diagnosis must win — the prose is not the query.
        assert_eq!(
            subcategory("_(x @ 1), # < 1 -- check x is null").as_deref(),
            Some("comment")
        );
        assert_eq!(
            subcategory("_(x @ 1), # < 1 -- was x * 2 + 1").as_deref(),
            Some("comment")
        );
    }

    #[test]
    fn sort_minus_fires_and_arithmetic_stays_silent() {
        assert_eq!(
            subcategory("_(x @ 1) |> #(-x)").as_deref(),
            Some("sort_minus")
        );
        // minus deeper in the window is arithmetic, not descending intent
        assert_ne!(
            subcategory("_(x @ 1) |> #(0 - x) is").as_deref(),
            Some("sort_minus")
        );
    }

    #[test]
    fn dash_comment_silent_on_gap_and_inside_string() {
        // `- -` with a gap is subtraction of a negative, not a comment.
        assert_ne!(
            subcategory("_(x @ 1), x - - 1 = 3").as_deref(),
            Some("comment")
        );
        // `--` inside a string literal is ordinary text; the real mistake
        // here is `is null` and that diagnosis must still win.
        assert_eq!(
            subcategory("_(s @ \"a--b\"), s is null").as_deref(),
            Some("is_null")
        );
    }

    #[test]
    fn multibyte_recovery_text_never_panics() {
        // Recovery text is user-controlled; the lexer must hold char
        // boundaries. Accented, CJK, and emoji, in error spans and gaps.
        let _ = subcategory("_(x @ 1), x * é + 1");
        let _ = subcategory("_(x @ 1), x * 日本語 + 1");
        let _ = subcategory("_(x @ 1), x * 🎉 + 1");
        let _ = subcategory("é日🎉");
        let _ = subcategory("_(é @ 1), é is null");
    }

    #[test]
    fn diagnosis_still_fires_after_multibyte_text() {
        // Multibyte text ahead of the pattern must not derail token
        // positions: the `--` pair is still adjacent, the `#(-` window
        // still matches.
        assert_eq!(
            subcategory("_(x @ \"café\"), # < 1 -- note").as_deref(),
            Some("comment")
        );
        assert_eq!(
            subcategory("_(x @ \"café\") |> #(-x)").as_deref(),
            Some("sort_minus")
        );
    }
}
