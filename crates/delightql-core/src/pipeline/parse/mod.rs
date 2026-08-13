// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Phase 0: text → typed CST.
//!
//! The compiler's ONE road from bytes to a tree. Every entry here names the
//! category the host already knows and hands that category to
//! [`crate::pipeline::syntax`]; nothing reads the text to decide which form it
//! is. The grammar's branches OVERLAP on purpose — `f(1, 2)` is a fact in a
//! definition file and an argumentative query in a sequence, identical bytes —
//! so a road that guessed would be answering a question the caller had already
//! answered.
//!
//! Every tree this module hands out has been measured against a nesting
//! budget and checked for defects. That is one road on purpose: a check
//! written into each entry is a rule every new entry has to remember, and an
//! entry that forgets it hands an unchecked tree to a recursive consumer —
//! which aborts the host process rather than answering.
//!
//! WHOSE budget is always the running compilation's. An entrance handed a
//! [`NestingBudget`] is one whose caller holds the compilation's arena and can
//! say so directly; every other entrance asks
//! [`NestingBudget::current`], which answers with the compilation running on
//! this thread and falls back to process policy only when none is. So a parse
//! reached from deep inside — a stored definition read back during resolution,
//! a companion cell, a compiler helper that mints its own arena — is measured
//! against the same depth `sys::execution.compiler_limit(*)` publishes for
//! that compilation, and only a tooling entrance with no compilation at all
//! reads policy at its door.

use crate::compiler_limits::NestingBudget;
use crate::error::{DelightQLError, Result};
use crate::pipeline::syntax::{cst, CompanionColumn, Defect, DefectKind, Parser, Root, SyntaxTree};

pub(crate) mod diagnosis;
pub mod nesting;

/// One interactive submission: the prompt wraps it as a top-level goal.
///
/// A single query piped into the CLI and a REPL line take the same road. The
/// wrap keeps interactive convenience OUTSIDE the grammar while the parser
/// still receives canonical text.
pub fn prompt(source: &str) -> Result<SyntaxTree> {
    prompt_attributed(source).map_err(|refusal| refusal.error)
}

/// The same, tolerating defects. For `--to cst`, whose whole job is to SHOW a
/// bad parse. The budget still applies: rendering walks the tree recursively,
/// so a tree too deep to walk is refused here exactly as anywhere else.
pub fn prompt_showing_defects(source: &str) -> Result<SyntaxTree> {
    let tree = Parser::new().parse_prompt(source);
    within_budget(&tree, NestingBudget::current())?;
    Ok(tree)
}

/// One whole submission, at the entrance the submission NAMES.
///
/// THE MARK IS THE CATEGORY, and [`Parser::parse_submission`] is the one place
/// that reads it. The three framing states — an authored header, a misplaced
/// one, no header at all — are the syntax crate's answer, and a host that
/// reduced them to "is it marked?" before parsing would lose the middle one:
/// misframed text would be read as an interactive prompt and refuse for a
/// reason the author did not write. Nothing on this side scans for the header.
///
/// Unmarked text is one interactive submission and takes the prompt wrap.
pub fn submission(source: &str, budget: NestingBudget) -> Result<SyntaxTree> {
    submission_attributed(source, budget).map_err(|refusal| refusal.error)
}

/// The same, tolerating defects, for `--to cst`.
pub fn submission_showing_defects(source: &str, budget: NestingBudget) -> Result<SyntaxTree> {
    let tree = Parser::new().parse_submission(source);
    within_budget(&tree, budget)?;
    Ok(tree)
}

/// The same, with its refusal attributed.
///
/// The tree records which road it took, so the owner is drawn the way that
/// road draws it — read back, never re-decided.
pub(crate) fn submission_attributed(
    source: &str,
    budget: NestingBudget,
) -> std::result::Result<SyntaxTree, AttributedRefusal> {
    let tree = Parser::new().parse_submission(source);
    let owner = match tree.entrance() {
        Root::QuerySequence => failing_form(&tree),
        // A prompt carries ONE goal, so that goal's extent is the owner.
        Root::DefinitionFile | Root::CompanionCell => submission_extent(&tree),
    };
    attributed(tree, Category::Query, owner, budget)
}

/// The utility entrance: a sequence of bare queries to run in order.
pub fn query_sequence(source: &str) -> Result<SyntaxTree> {
    query_sequence_attributed(source, NestingBudget::current()).map_err(|refusal| refusal.error)
}

/// The same, tolerating defects. For a caller that DIVIDES a submission: the
/// division is what says which statement a defect belongs to, so refusing the
/// whole submission first would hand every statement one statement's failure.
/// The budget still applies — a tree too deep to walk is refused here as
/// anywhere else.
pub fn query_sequence_showing_defects(source: &str) -> Result<SyntaxTree> {
    let tree = Parser::new().parse_query_sequence(source);
    within_budget(&tree, NestingBudget::current())?;
    Ok(tree)
}

/// The AUTHORED extent of each statement in a submission, in order.
///
/// The boundaries are the STARTS of the forms recovery proved, and the extents
/// TILE: every byte the author wrote belongs to exactly one of them. A splitter
/// that handed back only the proven spans would drop what recovery could not
/// attach — a trailing group it never closed, the comment between two
/// statements — and the pieces would no longer be the submission.
///
/// Text before the first proven form joins it, and text after a proven form
/// joins THAT one, because a form recovery proved up to a point and then
/// abandoned is the form the abandoned text was written into. Whether the
/// piece's own hook may then catch its own refusal is not decided here: each
/// piece is read again, and [`owning_form`] draws the finer boundary inside it.
pub(crate) fn statement_extents(tree: &SyntaxTree) -> Vec<std::ops::Range<usize>> {
    let proven = query_spans(tree);
    let end_of_source = tree.source().len();
    if proven.is_empty() {
        // Nothing was proven. A submission with a defect is still one
        // statement — the one that failed; a submission with none holds no
        // statement at all, only blank text and comments.
        return if tree.has_defects() && end_of_source > 0 {
            vec![0..end_of_source]
        } else {
            Vec::new()
        };
    }
    let mut extents = Vec::with_capacity(proven.len());
    for (index, span) in proven.iter().enumerate() {
        let start = if index == 0 { 0 } else { span.start };
        let end = proven
            .get(index + 1)
            .map_or(end_of_source, |next| next.start);
        extents.push(start..end);
    }
    extents
}

/// The canonical entrance: definitions and explicit `?-` goals.
pub fn definition_file(source: &str) -> Result<SyntaxTree> {
    checked(
        Parser::new().parse_definition_file(source),
        Category::Definitions,
        NestingBudget::current(),
    )
}

/// Normalize a parsed sequence with a fresh identity arena.
///
/// The fuzzer's entrance: it asks whether the road from bytes to AST holds for
/// a generated form, and has no session to share an arena with.
pub fn normalize_sequence(tree: &SyntaxTree) -> Result<crate::pipeline::normalize::Normalized> {
    crate::pipeline::normalize::query_sequence(
        tree,
        std::rc::Rc::new(crate::names::Registry::new(&[])),
    )
}

/// One companion cell, at the root its COLUMN selects.
pub fn companion_cell(column: CompanionColumn, cell: &str) -> Result<SyntaxTree> {
    checked(
        Parser::new().parse_companion_cell(column, cell),
        Category::Companion,
        NestingBudget::current(),
    )
}

/// The AUTHORED extent of each query in a sequence, in order.
///
/// The forms are structural. A text scan for a separator would have to know
/// which `\n` is inside a template and which ends a query — a question the
/// parse has already answered.
///
/// Asked of DEFECTIVE trees on purpose: the boundaries recovery could still
/// prove are what attributes a refusal. So the forms are read as the
/// OUTERMOST members wherever they stand, not as the children of a recognized
/// sequence — recovery is free to leave a proven form under an unrecognized
/// root, and a reader keyed on the root's shape would then divide a submission
/// it can plainly see the pieces of into one undivided extent, handing the
/// first form's declaration to the last form's failure.
pub(crate) fn query_spans(tree: &SyntaxTree) -> Vec<std::ops::Range<usize>> {
    crate::pipeline::syntax::outermost(tree, &[cst::Kind::Relex, cst::Kind::Effrelex])
        .filter_map(|form| tree.byte_range(form))
        .collect()
}

/// The extent a DEFECT belongs to, drawn by what the parse PROVED.
///
/// A defect lying inside a proven form belongs to that form. A defect outside
/// every proven form belongs to the region the proven forms leave around it —
/// from the end of the last one before it to the start of the first one after
/// it — because that region holds only text no form claimed.
///
/// That is what keeps one query from answering for another. A PROVEN sibling's
/// tokens are never inside the region, so no teaching and no declaration can
/// travel from one; recovery is what refused to divide the region further, and
/// the extent says exactly as much as the parse does. When recovery proved no
/// form at all, the region is the whole submission — there is no proven form
/// anywhere to have lent anything.
///
/// The defect's RANGE decides, not its first byte. Recovery is free to leave a
/// defect spanning forms it recognized nonetheless, and such a defect starts at
/// the first byte of text that parsed — reading only that byte would hand the
/// first form the last form's failure. What the defect actually failed on is
/// the text inside it that no form claimed.
fn owning_form(
    tree: &SyntaxTree,
    defect: &std::ops::Range<usize>,
) -> Option<std::ops::Range<usize>> {
    let spans = query_spans(tree);
    let at =
        unclaimed_start(tree.source(), defect, &spans, &separators(tree)).unwrap_or(defect.start);
    if let Some(span) = spans
        .iter()
        .find(|span| span.start <= at && defect.end <= span.end)
    {
        return Some(span.clone());
    }
    let preceding = spans
        .iter()
        .filter(|span| span.end <= at)
        .map(|span| span.end)
        .max();
    let following = spans
        .iter()
        .filter(|span| span.start > at)
        .min_by_key(|span| span.start);
    match (preceding, following) {
        // TEXT BEFORE EVERY PROVEN FORM WAS WRITTEN INTO IT. No earlier form
        // exists for it to have come from, so the region reaches THROUGH the
        // form it precedes — the same join the splitter makes. Recovery is
        // free to prove the tail of a line and abandon its head; drawing the
        // region short of the form would hand the head's failure a region
        // holding neither the query's own teaching nor its declaration.
        (None, Some(next)) => Some(0..next.end),
        (preceding, next) => {
            let start = preceding.unwrap_or(0);
            let end = next.map_or_else(|| tree.source().len(), |span| span.start);
            (start < end).then_some(start..end)
        }
    }
}

/// The text that SEPARATES forms rather than belonging to one: the grammar's
/// extras and the utility file's own reader directive.
///
/// A comment before the first query is not what that query failed on, any
/// more than the newline before it is. Counting one as unclaimed text draws
/// the failing region around a preamble, and every teaching keyed on what the
/// author typed then reads a comment instead of a query.
fn separators(tree: &SyntaxTree) -> Vec<std::ops::Range<usize>> {
    let mut spans: Vec<std::ops::Range<usize>> = crate::pipeline::syntax::walk(tree)
        .filter(|node| {
            matches!(
                node.typed_kind(),
                Some(
                    cst::Kind::Comment
                        | cst::Kind::SmartComment
                        | cst::Kind::StopPoint
                        | cst::Kind::DebugPoint
                        | cst::Kind::QuerySequenceHeader
                )
            )
        })
        .filter_map(|node| tree.byte_range(node))
        .collect();
    spans.sort_by_key(|span| span.start);
    spans
}

/// Where inside a defect the text no proven form claimed begins.
///
/// Whitespace-only stretches are not it: a newline between two forms is what
/// separates them, not what either failed on — and neither is a comment or the
/// file's reader directive. `None` when the forms tile the defect, which means
/// the defect is theirs.
fn unclaimed_start(
    source: &str,
    defect: &std::ops::Range<usize>,
    spans: &[std::ops::Range<usize>],
    separators: &[std::ops::Range<usize>],
) -> Option<usize> {
    let claimed = |from: usize, to: usize| -> bool {
        let mut at = from;
        for separator in separators {
            if separator.end <= at || separator.start >= to {
                continue;
            }
            if !source[at..separator.start.min(to)].trim().is_empty() {
                return false;
            }
            at = at.max(separator.end);
        }
        at >= to || source[at.min(to)..to].trim().is_empty()
    };
    let mut at = defect.start;
    for span in spans {
        if span.end <= at || span.start >= defect.end {
            continue;
        }
        if span.start > at && !claimed(at, span.start.min(defect.end)) {
            return Some(at);
        }
        at = at.max(span.end);
    }
    (at < defect.end && !claimed(at, defect.end)).then_some(at)
}

/// The extent a PROMPT submission's one query occupies.
///
/// A prompt is one statement by contract, so every byte of it belongs to that
/// statement and the extent is the whole authored text. It is deliberately
/// NOT the recovered goal node: recovery ends the goal at the failure, and a
/// teaching keyed on what the author typed lives in the bytes past it — the
/// `is null` a query was refused for is outside the `users(*)` that survived.
///
/// `None` when the tree shows TWO OR MORE top-level forms. A text holding
/// more than one form is not the submission this contract describes, and
/// saying nothing is the closed answer.
pub(crate) fn submission_extent(tree: &SyntaxTree) -> Option<std::ops::Range<usize>> {
    match tree.root_branch_if_shaped() {
        Some(cst::SourceFileChild::DefinitionFile(file)) if file.children().take(2).count() > 1 => {
            None
        }
        // A marked file may carry many forms; one submission that does owns
        // no single extent, exactly as a many-goal prompt does not.
        Some(cst::SourceFileChild::QuerySequenceRoot(_)) if query_spans(tree).len() > 1 => None,
        _ => Some(0..tree.source().len()),
    }
}

/// What the caller was reading, for the badge a refusal carries. The three
/// differ only in how a failure is CLASSIFIED — a consult error and a query
/// error are told apart by tooling that never sees the tree.
#[derive(Clone, Copy)]
enum Category {
    Query,
    Definitions,
    Companion,
}

impl Category {
    fn subcategory(self) -> Option<&'static str> {
        match self {
            Category::Query => None,
            Category::Definitions => Some(crate::uri_registry::subcat::PARSE_DDL),
            Category::Companion => Some(crate::uri_registry::subcat::PARSE_SIGIL),
        }
    }
}

fn checked(tree: SyntaxTree, category: Category, budget: NestingBudget) -> Result<SyntaxTree> {
    within_budget(&tree, budget)?;
    if tree.has_defects() {
        return Err(refusal(&tree, category, None));
    }
    Ok(tree)
}

/// A parse refusal with the QUERY it belongs to named, and the tree it was
/// read from.
///
/// DIAGNOSIS AND OWNERSHIP ARE ONE ACT. The teaching patterns key on the
/// tokens the author typed, so a scan wider than the failing form can report
/// what a SIBLING form spelled — and a caller that then weighs that identity
/// against the failing form's declaration lets one query answer for another.
/// The extent that selects the message is the extent that owns it.
pub(crate) struct AttributedRefusal {
    pub error: DelightQLError,
    pub query: Option<std::ops::Range<usize>>,
    pub tree: SyntaxTree,
}

/// The refusal a defective tree makes, attributed to the form that failed.
///
/// `owner` is the caller's structural reading of which form the first defect
/// stands in. `None` — a defect between two proven forms, or a submission
/// recovery could not divide — leaves the message the whole tree's and the
/// refusal owned by nothing, which is the closed answer: no declaration is
/// consulted for it anywhere.
fn attributed(
    tree: SyntaxTree,
    category: Category,
    owner: Option<std::ops::Range<usize>>,
    budget: NestingBudget,
) -> std::result::Result<SyntaxTree, AttributedRefusal> {
    if let Some(error) = nesting::refuse_if_over(budget, tree.depth()) {
        // A budget is the whole tree's measurement; no form owns it.
        return Err(AttributedRefusal {
            error,
            query: None,
            tree,
        });
    }
    if !tree.has_defects() {
        return Ok(tree);
    }
    let error = refusal(&tree, category, owner.as_ref());
    Err(AttributedRefusal {
        error,
        query: owner,
        tree,
    })
}

/// The utility entrance, with its refusal attributed. One parse: the tokens
/// that choose the message, the extent that owns it, and the tree a
/// declaration is read from are all this one.
pub(crate) fn query_sequence_attributed(
    source: &str,
    budget: NestingBudget,
) -> std::result::Result<SyntaxTree, AttributedRefusal> {
    let tree = Parser::new().parse_query_sequence(source);
    let owner = failing_form(&tree);
    attributed(tree, Category::Query, owner, budget)
}

/// The form a sequence's refusal belongs to: the one the FIRST defect stands
/// in.
///
/// `None` when no form within the submission owns it. A misplaced header is
/// the framing decision for the WHOLE submission — it is why every other byte
/// was read the way it was — so no form inside may answer for it or lend it a
/// declaration, exactly as a nesting budget is the whole tree's measurement
/// and belongs to no form either.
fn failing_form(tree: &SyntaxTree) -> Option<std::ops::Range<usize>> {
    let first = tree.defects().into_iter().next()?;
    if first.kind == DefectKind::MisplacedHeader {
        return None;
    }
    owning_form(tree, &first.byte_range)
}

/// The interactive entrance, with its refusal attributed. A prompt carries
/// ONE goal, so that goal's extent is the owner when the tree still shows it.
pub(crate) fn prompt_attributed(
    source: &str,
) -> std::result::Result<SyntaxTree, AttributedRefusal> {
    let tree = Parser::new().parse_prompt(source);
    let owner = submission_extent(&tree);
    attributed(tree, Category::Query, owner, NestingBudget::current())
}

/// Depth is refused before a recursive walk or it is not refused at all.
/// tree-sitter builds its tree iteratively, so the measurement costs no stack.
fn within_budget(tree: &SyntaxTree, budget: NestingBudget) -> Result<()> {
    match nesting::refuse_if_over(budget, tree.depth()) {
        Some(refusal) => Err(refusal),
        None => Ok(()),
    }
}

/// What a defective parse says, in the order a reader can use.
///
/// The teaching patterns come first: they key on the tokens the author typed
/// and fire only when unambiguous, so when one speaks it is the most specific
/// true thing available. A homoglyph reading comes next — the character is
/// invisible and no other message would name it. The positional message is the
/// floor, and says only what it knows.
fn refusal(
    tree: &SyntaxTree,
    category: Category,
    within: Option<&std::ops::Range<usize>>,
) -> DelightQLError {
    let source = tree.source();
    let subcategory = category.subcategory();

    // A MISPLACED HEADER OUTRANKS EVERYTHING. It is why the rest of the file
    // was read the way it was, so whatever the misreading produced downstream
    // describes a consequence rather than the cause. It is also not filtered
    // by the owning form: the header decides how the whole submission is
    // framed, so it belongs to no single form within it.
    if let Some(defect) = tree
        .defects()
        .iter()
        .find(|defect| defect.kind == DefectKind::MisplacedHeader)
    {
        let (row, column) = line_and_column(source, defect.byte_range.start);
        return DelightQLError::ParseError {
            message: format!(
                "Parse error at line {row}:{column}\n\
                 {header} must be the first nonblank line",
                header = delightql_cst::QUERY_SEQUENCE_HEADER,
            ),
            source: None,
            subcategory,
        };
    }

    // WHAT THE FAILING FORM TYPED, and nothing a sibling typed. A teaching
    // read from another form's tokens describes a query that did not fail.
    let inside = |start: usize, end: usize| match within {
        Some(extent) => start >= extent.start && end <= extent.end,
        None => true,
    };
    let tokens: Vec<_> = tree
        .tokens()
        .into_iter()
        .filter(|token| inside(token.start, token.end))
        .collect();
    if let Some(found) = diagnosis::diagnose(&tokens, source) {
        return DelightQLError::ParseError {
            message: found.message,
            source: None,
            subcategory: Some(found.subcategory),
        };
    }

    let defects: Vec<_> = tree
        .defects()
        .into_iter()
        .filter(|defect| inside(defect.byte_range.start, defect.byte_range.end))
        .collect();
    if let Some(message) = defects.iter().find_map(|d| homoglyph(d, source)) {
        return DelightQLError::ParseError {
            message,
            source: None,
            subcategory,
        };
    }

    // A MISSING token names what the grammar required, which is more specific
    // than an unparsed span; prefer it when both are present.
    let missing = defects
        .iter()
        .find(|d| d.kind == DefectKind::Missing)
        .or_else(|| defects.first());

    let message = match missing {
        Some(defect) => {
            let position = defect.byte_range.start;
            let context = context_around(source, position);
            match (&defect.kind, &defect.expected) {
                (DefectKind::Missing, Some(expected)) => format!(
                    "Syntax error at line {}:{}: expected '{expected}' but found end of \
                     input or unexpected token\nContext: '{context}'",
                    defect.start.row + 1,
                    defect.start.column + 1,
                ),
                _ => format!(
                    "Syntax error at line {}:{}: expected valid DelightQL syntax near '{context}'",
                    defect.start.row + 1,
                    defect.start.column + 1,
                ),
            }
        }
        None => "Parse tree contains errors - syntax is invalid".to_string(),
    };

    DelightQLError::ParseError {
        message,
        source: None,
        subcategory,
    }
}

/// A Unicode confusable standing where its ASCII twin belongs.
///
/// Read off the defect's own bytes rather than a recovery node kind: which
/// node carries an unexpected character is a recovery detail, while the
/// character itself is what the author pasted. The position reported is the
/// CHARACTER's, not the defect's — a recovery span can open well before the
/// byte the author needs to look at.
fn homoglyph(defect: &Defect, source: &str) -> Option<String> {
    let text = source.get(defect.byte_range.clone())?;
    text.char_indices().find_map(|(offset, ch)| {
        let found = lookup_homoglyph(ch)?;
        let (row, column) = line_and_column(source, defect.byte_range.start + offset);
        Some(format!(
            "Parse error at line {row}:{column}\n{found}\n\nHint: This often happens when \
             copy-pasting from formatted documents.",
        ))
    })
}

/// The one-based line and column of a byte offset, counted in CHARACTERS the
/// way every other position this module reports is.
fn line_and_column(source: &str, offset: usize) -> (usize, usize) {
    let before = &source[..floor_char_boundary(source, offset)];
    let row = before.matches('\n').count() + 1;
    let column = before
        .rfind('\n')
        .map_or(before, |at| &before[at + 1..])
        .chars()
        .count()
        + 1;
    (row, column)
}

fn lookup_homoglyph(ch: char) -> Option<String> {
    const DASH_REMEDY: &str = "Expected '-' (U+002D HYPHEN-MINUS)\n\nUse ASCII hyphen-minus in \
                               syntax (operators, definition necks, etc.)";
    let named = match ch {
        '\u{2212}' => "U+2212 MINUS SIGN",
        '\u{2013}' => "U+2013 EN DASH",
        '\u{2014}' => "U+2014 EM DASH",
        '\u{2010}' => "U+2010 HYPHEN",
        '\u{03BF}' => {
            return Some(
                "Found 'ο' (U+03BF GREEK SMALL LETTER OMICRON)\nExpected 'o' \
                 (U+006F LATIN SMALL LETTER O)"
                    .to_string(),
            )
        }
        '\u{0430}' => {
            return Some(
                "Found 'а' (U+0430 CYRILLIC SMALL LETTER A)\nExpected 'a' \
                 (U+0061 LATIN SMALL LETTER A)"
                    .to_string(),
            )
        }
        _ => return None,
    };
    Some(format!("Found '{ch}' ({named})\n{DASH_REMEDY}"))
}

/// Clamp `index` to the nearest char boundary at or below it.
///
/// std's `str::floor_char_boundary` is still unstable; same contract. Error
/// positions are byte offsets, and arbitrary byte arithmetic on them (context
/// windows, display truncation) can land inside a multi-byte character —
/// slicing there panics.
pub(crate) fn floor_char_boundary(s: &str, index: usize) -> usize {
    if index >= s.len() {
        return s.len();
    }
    let mut i = index;
    while !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// Truncate `text` to at most `max_bytes` bytes for display, appending "..."
/// when truncated, never splitting a multi-byte character.
pub(crate) fn truncate_for_display(text: &str, max_bytes: usize) -> String {
    if text.len() > max_bytes {
        format!("{}...", &text[..floor_char_boundary(text, max_bytes)])
    } else {
        text.to_string()
    }
}

fn context_around(source: &str, position: usize) -> String {
    const CONTEXT_SIZE: usize = 20;
    let start = floor_char_boundary(source, position.saturating_sub(CONTEXT_SIZE));
    let end = floor_char_boundary(source, (position + CONTEXT_SIZE).min(source.len()));
    source[start..end]
        .chars()
        .map(|c| if c == '\n' { ' ' } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Submissions that NAME the utility entrance and put the mark somewhere
    /// the placement law does not admit.
    ///
    /// Each is a header the reader recognizes: the recognition is deliberately
    /// wider than the acceptance, so an indented header or one behind a
    /// byte-order mark is a placement to teach about rather than unrecognized
    /// text failing obscurely somewhere downstream.
    const MISFRAMED: &[(&str, &str)] = &[
        (
            "a comment before it",
            "// note\n#!dql query-sequence\nusers(*)\n",
        ),
        (
            "indentation before it",
            "  #!dql query-sequence\nusers(*)\n",
        ),
        (
            "a byte-order mark before it",
            "\u{feff}#!dql query-sequence\nusers(*)\n",
        ),
        (
            "a second one",
            "#!dql query-sequence\n#!dql query-sequence\nusers(*)\n",
        ),
        ("query text before it", "users(*)\n#!dql query-sequence\n"),
    ];

    /// The teaching a misplaced header earns.
    const PLACEMENT: &str = "must be the first nonblank line";

    /// A MISPLACED MARK IS STILL THE MARK.
    ///
    /// The submission road reads every one of these as a query sequence and
    /// teaches the placement. Reading one as an interactive prompt instead
    /// would answer a placement error with an unrelated syntax error, and the
    /// teaching would have no road to a host at all.
    #[test]
    fn a_misplaced_header_teaches_the_placement_law() {
        for (why, source) in MISFRAMED {
            let Err(error) = submission(source, NestingBudget::current()) else {
                panic!("{why}: a misplaced header is a refusal");
            };
            assert!(error.to_string().contains(PLACEMENT), "{why}: {error}");
        }
    }

    /// The same submissions, read the same way, when the caller wants the
    /// defective tree rather than the refusal.
    #[test]
    fn a_misplaced_header_keeps_its_framing_when_defects_are_shown() {
        for (why, source) in MISFRAMED {
            let tree = submission_showing_defects(source, NestingBudget::current())
                .unwrap_or_else(|error| panic!("{why}: showing defects does not refuse: {error}"));
            assert_eq!(
                tree.entrance(),
                Root::QuerySequence,
                "{why}: the mark names the utility entrance"
            );
            assert!(
                tree.defects()
                    .iter()
                    .any(|defect| defect.kind == DefectKind::MisplacedHeader),
                "{why}: {:?}",
                tree.defects()
            );
        }
    }

    /// The placement refusal is the WHOLE submission's, so no form inside it
    /// owns the refusal or may lend it a declaration.
    #[test]
    fn a_misplaced_header_belongs_to_no_form_within_the_submission() {
        for (why, source) in MISFRAMED {
            let Err(refusal) = submission_attributed(source, NestingBudget::current()) else {
                panic!("{why}: a misplaced header is a refusal");
            };
            assert!(
                refusal.error.to_string().contains(PLACEMENT),
                "{why}: {}",
                refusal.error
            );
            assert!(
                refusal.query.is_none(),
                "{why}: the framing decision is no form's, but got {:?}",
                refusal.query
            );
        }
    }

    /// The two lawful shapes, each on its own road: a placed mark is a
    /// sequence, and unmarked text is one interactive submission.
    #[test]
    fn a_placed_header_and_unmarked_text_take_their_own_roads() {
        let marked = submission("#!dql query-sequence\nusers(*)\n", NestingBudget::current())
            .expect("a placed header is lawful");
        assert_eq!(marked.entrance(), Root::QuerySequence);
        assert!(!marked.has_defects(), "{:?}", marked.defects());

        let unmarked = submission("users(*)\n", NestingBudget::current())
            .expect("unmarked text is one submission");
        assert_eq!(unmarked.entrance(), Root::DefinitionFile);
        assert!(!unmarked.has_defects(), "{:?}", unmarked.defects());
    }

    #[test]
    fn a_naked_query_refuses_canonically_and_stands_in_a_sequence() {
        assert!(definition_file("users(*)").is_err());
        assert!(query_sequence("users(*)").is_ok());
        assert!(prompt("users(*)").is_ok());
    }

    #[test]
    fn error_context_is_char_boundary_safe() {
        // A multi-byte character straddling the context window's edge.
        let source = format!("{}é{}", "x".repeat(19), "y".repeat(30));
        let _ = context_around(&source, 20);
        let _ = context_around(&source, 0);
        let _ = context_around(&source, source.len());
    }

    #[test]
    fn a_homoglyph_names_the_character_it_found() {
        let Err(error) = prompt("users(*), a \u{2212} b = 1") else {
            panic!("a confusable where an operator belongs does not parse");
        };
        assert!(
            error.to_string().contains("U+2212"),
            "the minus-sign confusable should be named: {error}"
        );
    }
}
