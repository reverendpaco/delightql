// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! The typed CST boundary for the consolidated DelightQL grammar.
//!
//! The typed CST describes what the author WROTE — source spans, tooling
//! tokens, alternate spellings, concrete structure. The semantic AST describes
//! what the program MEANS. Deciding which spelling distinctions disappear is
//! CST-to-AST normalization's job and nothing downstream re-decides it.
//!
//! Everything under [`cst`] is generated into `OUT_DIR` from the grammar's
//! `node-types.json`. This module is the whole checked-in surface: a small,
//! stable façade over generated code that the grammar owns.
//!
//! The property worth the machinery is exhaustiveness. Each supertype in the
//! grammar becomes a Rust enum, so a consumer that matches on
//! [`cst::Continuation`] cannot silently miss a member — adding a continuation
//! form to the grammar becomes a compile error in every consumer instead of a
//! branch that quietly does nothing. That is the failure mode a `node.kind()`
//! string comparison cannot prevent, and it is why this boundary exists.
//!
//! ## Entrances
//!
//! Every parse names the category the host already knows. The grammar's
//! branches OVERLAP — `f(1, 2)` is a fact in the canonical form and an
//! argumentative query in the utility one, identical bytes — so nothing here
//! guesses which the caller meant. [`Parser::parse_definition_file`] is the
//! canonical entrance and refuses a naked query; the other three prepend a
//! host-only selector.
//!
//! ## Coordinates
//!
//! A selector is text the author never wrote, so a raw Tree-sitter range points
//! into a string that does not exist on disk. [`SyntaxTree`] keeps the mapping
//! and answers in AUTHORED coordinates: [`SyntaxTree::source`] is the
//! submission, [`SyntaxTree::text`] and [`SyntaxTree::byte_range`] are measured
//! against it, and a defect at the first authored byte reports byte zero. The
//! raw accessors on [`TypedNode`] carry `raw_` in their names for the same
//! reason.

use tree_sitter::{Language, Node, Point, Tree};

/// The generated typed API.
// Generated code: an accessor exists for every field the grammar declares,
// whether or not this repository calls it yet — that is the inventory G.2
// consumes.
#[allow(clippy::all)]
#[allow(dead_code, unused_imports, non_snake_case)]
pub mod cst {
    include!(concat!(env!("OUT_DIR"), "/typed_cst.rs"));
}

pub use cst::{AnyNode, Kind, TypedNode, EXTRA_KINDS};

// `GRAMMAR_FINGERPRINT` and `PARSER_RUNTIME`: the build identity the parser
// evidence records, minted by the build script beside the generated CST.
include!(concat!(env!("OUT_DIR"), "/minted_facts.rs"));

extern "C" {
    fn tree_sitter_delightql() -> Language;
}

/// The one consolidated language. There is no second one to choose between.
pub fn language() -> Language {
    unsafe { tree_sitter_delightql() }
}

/// The entrance a host asked for.
///
/// The definition file is the canonical language form and the default: every
/// query in it begins with `?-`. The query sequence is a utility form for
/// execution tools. The companion cell is the DDL sigil sub-language, whose
/// root is selected by companion COLUMN — never by reading the cell.
///
/// This is what the CALLER named, not what the text turned out to be. The two
/// can only differ when the text is defective, and then the entrance is still
/// the truth about how it was read.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Root {
    DefinitionFile,
    QuerySequence,
    CompanionCell,
}

/// Which companion column a cell came from. The column decides the parse root;
/// no reader ever classifies a cell by what is inside it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CompanionColumn {
    Constraint,
    Default,
}

/// The utility file's own header. Unlike the selectors below it is AUTHORED
/// vocabulary: a file may carry it, and a raw consumer of the generated
/// language — an editor, a highlighter — reads it to know which world the file
/// is in. The host injects exactly these bytes when the submission omits them,
/// which is why there is one spelling and not two.
pub const QUERY_SEQUENCE_HEADER: &str = "#!dql query-sequence";

/// The header as the host writes it: the line, terminated. A newline rather
/// than a space, because the header is a LINE — its placement law is about
/// lines, and a same-line injection would put the author's first byte on a row
/// no coordinate could describe.
const QUERY_SEQUENCE_FRAME: &str = "#!dql query-sequence\n";

/// The host-only selectors. Each is text the HOST writes to name a category it
/// already knows, in the `?-` prompt wrap's family — never authored DelightQL,
/// and never visible in an authored coordinate.
const CONSTRAINT_CELL_SELECTOR: &str = "@constraint-cell ";
const DEFAULT_CELL_SELECTOR: &str = "@default-cell ";
const PROMPT_SELECTOR: &str = "?- ";

/// Where a submission's utility header stands, judged before parsing.
///
/// The placement law is the ruling's: first nonblank line, column zero, exact
/// spelling, once. A blank line may hold spaces and tabs; LF and CRLF are
/// both line endings. Nothing else may precede it — not a comment, not a BOM.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Framing {
    /// The author wrote the header where it belongs; the source is parsed as
    /// authored, and the header node has an authored range like any other.
    Authored,
    /// No header anywhere. The host injects one, and it has no authored range.
    Synthetic,
    /// A header stands somewhere the law does not admit. The submission is
    /// framed as if it had none, so every other defect still shows, and this
    /// one is reported at the offending header's own bytes.
    Misframed(std::ops::Range<usize>),
}

/// The header's placement in a submission.
///
/// A line COUNTS as a header when its content — leading whitespace and a
/// byte-order mark aside — is exactly the header. That is deliberately wider
/// than what is accepted: an indented header and a header behind a BOM are
/// misplaced headers to be taught about, not unrecognized text to fail
/// obscurely somewhere downstream.
fn framing(source: &str) -> Framing {
    let mut accepted: Option<std::ops::Range<usize>> = None;
    let mut leading = true;
    let mut at = 0usize;
    let mut misplaced: Option<std::ops::Range<usize>> = None;
    for line in source.split_inclusive('\n') {
        let end = at + line.trim_end_matches('\n').trim_end_matches('\r').len();
        let content = line.trim_end_matches('\n').trim_end_matches('\r');
        let bare = content
            .trim_start_matches([' ', '\t'])
            .trim_start_matches('\u{feff}')
            .trim_end_matches([' ', '\t']);
        if bare == QUERY_SEQUENCE_HEADER {
            let placed = leading && !content.starts_with([' ', '\t', '\u{feff}']);
            if placed && accepted.is_none() {
                accepted = Some(at..end);
            } else if misplaced.is_none() {
                misplaced = Some(at..end);
            }
        } else if !content.trim().is_empty() {
            leading = false;
        }
        at += line.len();
    }
    match (accepted, misplaced) {
        (_, Some(at)) => Framing::Misframed(at),
        (Some(_), None) => Framing::Authored,
        (None, None) => Framing::Synthetic,
    }
}


/// A parsed source.
///
/// Owns both the text that was PARSED and the offset of the author's first
/// byte within it, so every coordinate this hands out is one the author would
/// recognise.
pub struct SyntaxTree {
    tree: Tree,
    parsed: String,
    /// Bytes of host-supplied framing at the front of `parsed`.
    selector_len: usize,
    /// Rows of it. The utility header is a LINE, so a synthetic one shifts
    /// every row; the one-line selectors shift only row 0's columns.
    selector_rows: usize,
    /// A header the placement law does not admit, at its authored bytes. It
    /// is a defect the grammar cannot raise: extras are skipped before any
    /// token, so no production can require that nothing precede one.
    misframed: Option<std::ops::Range<usize>>,
    entrance: Root,
}

/// A syntax defect with the AUTHORED span that carries it.
#[derive(Clone, Debug)]
pub struct Defect {
    pub kind: DefectKind,
    /// The node kind Tree-sitter expected, when it names one.
    pub expected: Option<String>,
    /// Measured against the submission, not the parsed text.
    pub byte_range: std::ops::Range<usize>,
    pub start: Point,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DefectKind {
    /// Text the grammar admits nowhere here.
    Unparsed,
    /// A token the grammar requires, inserted by recovery rather than authored.
    Missing,
    /// The utility header stands where the placement law does not admit one.
    /// Not a grammar refusal, and it cannot be: extras are skipped before any
    /// token, so no production can say that nothing precedes this one.
    MisplacedHeader,
}

impl SyntaxTree {
    /// The text the author submitted, with no selector in front of it.
    pub fn source(&self) -> &str {
        &self.parsed[self.selector_len..]
    }

    /// The text that was parsed: selector followed by the submission. For
    /// diagnostics about the façade itself; a consumer wants [`Self::source`].
    pub fn parsed_source(&self) -> &str {
        &self.parsed
    }

    /// The host-only selector, empty for the canonical entrance.
    pub fn selector(&self) -> &str {
        &self.parsed[..self.selector_len]
    }

    /// The entrance the host asked for.
    pub fn entrance(&self) -> Root {
        self.entrance
    }

    pub fn raw(&self) -> &Tree {
        &self.tree
    }

    pub fn root(&self) -> cst::SourceFile<'_> {
        cst::SourceFile::cast(self.tree.root_node())
            .expect("the grammar's start rule is source_file")
    }

    /// The branch the tree actually carries. `None` for a canonical file that
    /// declares nothing — a blank or comments-only source is lawful and has no
    /// form to show.
    pub fn root_branch(&self) -> Option<cst::SourceFileChild<'_>> {
        self.root().child()
    }

    /// The same, for a tree that may be DEFECTIVE.
    ///
    /// Recovery can leave the root itself unrecognized, and a reader looking
    /// at a defective tree on purpose — to see what the author declared past
    /// the failure — must not be the one that discovers it. `None` says the
    /// tree shows no branch, which is the answer such a reader wants anyway.
    pub fn root_branch_if_shaped(&self) -> Option<cst::SourceFileChild<'_>> {
        cst::SourceFile::cast(self.tree.root_node()).and_then(|root| root.child())
    }

    /// A node's range in AUTHORED coordinates. `None` when the node lies
    /// wholly inside the host's selector; a node straddling the boundary —
    /// `source_file` itself — starts at the author's first byte.
    pub fn byte_range<'t, T: TypedNode<'t>>(&self, node: T) -> Option<std::ops::Range<usize>> {
        let raw = node.raw_byte_range();
        if raw.end <= self.selector_len {
            return None;
        }
        Some(self.authored_offset(raw.start)..self.authored_offset(raw.end))
    }

    /// The authored bytes under a node. Spans are preserved for exactly this
    /// reason: a spelling normalization drops is still readable here.
    pub fn text<'t, T: TypedNode<'t>>(&self, node: T) -> &str {
        match self.byte_range(node) {
            Some(range) => &self.source()[range],
            None => "",
        }
    }

    /// A node's start point in AUTHORED coordinates.
    pub fn start_position<'t, T: TypedNode<'t>>(&self, node: T) -> Option<Point> {
        let raw = node.raw_byte_range();
        if raw.end <= self.selector_len {
            return None;
        }
        Some(self.authored_point(node.raw_start_position(), raw.start))
    }

    /// A node's end point in AUTHORED coordinates.
    pub fn end_position<'t, T: TypedNode<'t>>(&self, node: T) -> Option<Point> {
        let raw = node.raw_byte_range();
        if raw.end <= self.selector_len {
            return None;
        }
        Some(self.authored_point(node.raw_end_position(), raw.end))
    }

    fn authored_offset(&self, raw: usize) -> usize {
        raw.saturating_sub(self.selector_len)
    }

    /// A point inside the framing is pulled forward to the author's first
    /// byte, matching `byte_range`.
    ///
    /// Two shapes of framing, one rule. A framing that ends in a newline puts
    /// the author's first byte at column zero of the next row, so only the row
    /// shifts. A one-line selector leaves the author on row 0, so only that
    /// row's columns shift.
    fn authored_point(&self, raw: Point, raw_offset: usize) -> Point {
        if raw_offset <= self.selector_len {
            return Point { row: 0, column: 0 };
        }
        let row = raw.row.saturating_sub(self.selector_rows);
        let column = if raw.row == self.selector_rows && self.selector_rows == 0 {
            raw.column.saturating_sub(self.selector_len)
        } else {
            raw.column
        };
        Point { row, column }
    }

    /// The deepest path in this tree, walked ITERATIVELY.
    ///
    /// A resource policy is measured here, and only here, because a recursive
    /// measurement would overflow on exactly the trees the measurement exists
    /// to catch. Tree-sitter builds its tree iteratively, so asking costs no
    /// stack; every recursive consumer downstream can then be refused BEFORE
    /// it descends. What the budget is, and what a refusal says, belong to the
    /// compiler — this is the reading, not the policy.
    pub fn depth(&self) -> usize {
        let mut cursor = self.tree.walk();
        let mut deepest = 1usize;
        // The cursor's own position is the walk state; `goto_first_child`
        // descends, `goto_next_sibling` moves across, `goto_parent` unwinds.
        let mut depth = 1usize;
        loop {
            if cursor.goto_first_child() {
                depth += 1;
                deepest = deepest.max(depth);
                continue;
            }
            loop {
                if cursor.goto_next_sibling() {
                    break;
                }
                if !cursor.goto_parent() {
                    return deepest;
                }
                depth -= 1;
            }
        }
    }

    /// Every leaf token in authored order, INCLUDING the text recovery
    /// dropped.
    ///
    /// A failed parse is where this matters. Tree-sitter often represents the
    /// offending bytes as nothing at all — a childless `ERROR` whose span
    /// covers them, or a gap between an `ERROR`'s surviving children — so a
    /// walk over named leaves alone cannot see what the author typed. The
    /// uncovered spans are re-lexed here, which is why this is the syntax
    /// crate's job: it is a reading of the SOURCE, and nothing about it
    /// classifies meaning.
    ///
    /// Tokens lying wholly inside the host's selector are not the author's
    /// and do not appear.
    pub fn tokens(&self) -> Vec<Token> {
        let mut raw = Vec::new();
        collect_tokens(self.tree.root_node(), &self.parsed, &mut raw);
        raw.sort_by_key(|token| token.start);
        raw.into_iter()
            .filter(|token| token.end > self.selector_len)
            .map(|token| Token {
                text: token.text,
                start: self.authored_offset(token.start),
                end: self.authored_offset(token.end),
                extra: token.extra,
            })
            .collect()
    }

    /// Every defect, innermost first within a subtree, in AUTHORED
    /// coordinates. A `MISSING` node matters as much as an `ERROR`: recovery
    /// inserting a token the author did not write means the text is not the
    /// text the tree describes.
    pub fn defects(&self) -> Vec<Defect> {
        let mut raw = Vec::new();
        collect_defects(self.tree.root_node(), &mut raw);
        let mut defects: Vec<Defect> = raw
            .into_iter()
            .map(|(kind, expected, range, start)| Defect {
                kind,
                expected,
                byte_range: self.authored_offset(range.start)..self.authored_offset(range.end),
                start: self.authored_point(start, range.start),
            })
            .collect();
        // The misplaced header comes FIRST whatever else the parse found: it
        // is the reason the rest of the file was read the way it was.
        if let Some(at) = &self.misframed {
            defects.insert(
                0,
                Defect {
                    kind: DefectKind::MisplacedHeader,
                    expected: None,
                    byte_range: at.clone(),
                    start: self.point_of(at.start),
                },
            );
        }
        defects
    }

    pub fn has_defects(&self) -> bool {
        self.misframed.is_some() || self.tree.root_node().has_error()
    }

    /// The authored row and column of an authored byte offset.
    fn point_of(&self, at: usize) -> Point {
        let mut point = Point { row: 0, column: 0 };
        for byte in self.source().as_bytes().iter().take(at) {
            if *byte == b'\n' {
                point.row += 1;
                point.column = 0;
            } else {
                point.column += 1;
            }
        }
        point
    }
}

/// One lexical token with its AUTHORED span.
#[derive(Clone, Debug)]
pub struct Token {
    pub text: String,
    pub start: usize,
    pub end: usize,
    /// Whether this token lies inside a node the grammar declares EXTRA.
    ///
    /// Admitted between any two tokens and contributing no structure, so a
    /// reader asking what comes NEXT steps over it. The classification is the
    /// grammar's — `EXTRA_KINDS` is generated from the same `extras` list —
    /// and a delimited extra marks its interior leaves too, since those
    /// stand between the two tokens the reader is relating.
    pub extra: bool,
}

/// Leaf tokens in parsed coordinates. An `ERROR` node's own bytes are re-lexed
/// where no child covers them, because those are exactly the bytes recovery
/// declined to give a node.
fn collect_tokens(node: Node<'_>, parsed: &str, out: &mut Vec<Token>) {
    collect_tokens_within(node, parsed, false, out)
}

fn collect_tokens_within(node: Node<'_>, parsed: &str, extra: bool, out: &mut Vec<Token>) {
    let mut cursor = node.walk();
    let children: Vec<Node<'_>> = node.children(&mut cursor).collect();
    // A delimited extra's OPENER, interior and CLOSER are separate leaves, so
    // the mark descends: each of them stands between the two tokens a reader
    // is relating, not only the node that names them together.
    let extra = extra || cst::EXTRA_KINDS.contains(&node.kind());

    if children.is_empty() {
        let text = &parsed[node.byte_range()];
        if node.is_error() || node.is_missing() {
            lex_recovery_text(text, node.start_byte(), out);
        } else if !text.is_empty() {
            // EMPTY, not blank. Whitespace BETWEEN tokens is an extra and has
            // no node, so it never arrives here — but a token whose text is
            // whitespace does: the space in `:"{first} {last}"` is a
            // `template_text`, and dropping it would let a reader conclude
            // two different templates were the same.
            out.push(Token {
                text: text.to_string(),
                start: node.start_byte(),
                end: node.end_byte(),
                extra,
            });
        }
        return;
    }

    if node.is_error() {
        let base = node.start_byte();
        let text = &parsed[node.byte_range()];
        let mut pos = 0usize;
        for child in &children {
            let start = child.start_byte() - base;
            if start > pos {
                lex_recovery_text(&text[pos..start], base + pos, out);
            }
            pos = child.end_byte() - base;
        }
        if text.len() > pos {
            lex_recovery_text(&text[pos..], base + pos, out);
        }
    }
    for child in &children {
        collect_tokens_within(*child, parsed, extra, out);
    }
}

/// Minimal lexer for recovery-dropped text: words, numbers, and single
/// characters. `char_indices` rather than byte arithmetic — the text is the
/// author's and slicing mid-codepoint panics.
///
/// Public because a defect diagnosis is only as good as the tokens it reads,
/// and there must be exactly one answer to "what did the author type here".
pub fn lex_recovery_text(text: &str, base: usize, out: &mut Vec<Token>) {
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
            // BYTES RECOVERY DECLINED TO GIVE A NODE have no kind, so they
            // are not classified as anything — including as an extra. An
            // unterminated `(/*` therefore stops a reader that steps over
            // extras, which is the fail-closed answer: the text after it is
            // inside a comment the author never closed.
            extra: false,
        });
    }
}

type RawDefect = (DefectKind, Option<String>, std::ops::Range<usize>, Point);

fn collect_defects(node: Node<'_>, out: &mut Vec<RawDefect>) {
    if !node.has_error() && !node.is_missing() {
        return;
    }
    if node.is_missing() {
        out.push((
            DefectKind::Missing,
            Some(node.kind().to_string()),
            node.byte_range(),
            node.start_position(),
        ));
        return;
    }
    let mut cursor = node.walk();
    let children: Vec<Node<'_>> = node.children(&mut cursor).collect();
    let mut descended = false;
    for child in children {
        if child.has_error() || child.is_missing() {
            descended = true;
            collect_defects(child, out);
        }
    }
    if node.is_error() && !descended {
        out.push((
            DefectKind::Unparsed,
            None,
            node.byte_range(),
            node.start_position(),
        ));
    }
}

/// What a cancellable parse answered: a finished tree, or the cancellation
/// with the last progress the runtime reported before it. The elapsed time is
/// the caller's to measure — the clock that armed the predicate owns it.
pub enum CancellableParse {
    Completed(SyntaxTree),
    Cancelled {
        /// The parse's last reported progress in AUTHORED bytes, `None` when
        /// cancellation fired before the first checkpoint.
        last_progress_byte: Option<usize>,
        /// The road the cancelled parse was on — the same framing decision
        /// a completed tree records as its entrance. Evidence about a
        /// cancellation must not lose the entrance the request selected.
        entrance: Root,
    },
}

/// The road [`Parser::parse_submission`] will take for this source, from
/// the same framing law, without parsing: marked text (an authored header,
/// misplaced included — a submission that says which world it is in has
/// said so) is the utility entrance; unmarked text is one interactive
/// submission at the prompt wrap.
pub fn submission_road(source: &str) -> Root {
    match framing(source) {
        Framing::Synthetic => Root::DefinitionFile,
        Framing::Authored | Framing::Misframed(_) => Root::QuerySequence,
    }
}

/// A parser bound to the consolidated language.
pub struct Parser {
    inner: tree_sitter::Parser,
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser {
    pub fn new() -> Self {
        let mut inner = tree_sitter::Parser::new();
        inner
            .set_language(&language())
            .expect("the generated parser matches the pinned runtime");
        Parser { inner }
    }

    /// The canonical entrance: a definition file. Definitions and explicit
    /// top-level goals; a naked query refuses. A file declaring nothing —
    /// blank, or only comments — is lawful.
    pub fn parse_definition_file(&mut self, source: &str) -> SyntaxTree {
        self.parse_with("", source, Root::DefinitionFile)
    }

    /// The utility entrance: a sequence of bare queries to run in order.
    ///
    /// This is a category the caller knows and the text cannot show. `f(1, 2)`
    /// submitted here is an argumentative query; the identical bytes submitted
    /// to the canonical entrance are a fact.
    ///
    /// ONE HEADER, WHOEVER WROTE IT. A submission that already carries the
    /// authored header is parsed as it stands — injecting a second would be a
    /// file with two, which the placement law refuses. A submission without
    /// one is framed with exactly the same bytes, so the tree a host builds
    /// and the tree an editor builds are the same tree.
    pub fn parse_query_sequence(&mut self, source: &str) -> SyntaxTree {
        self.frame_query_sequence(framing(source), source)
    }

    /// One whole submission, at the entrance the submission's own bytes NAME.
    ///
    /// THE MARK IS THE CATEGORY. `#!dql query-sequence` is a READER DIRECTIVE:
    /// its only job is to say how the bytes are read, so a host that ignored it
    /// would be re-deciding a question the author already closed. Nothing here
    /// reads the QUERY to guess its form — only the one directive the language
    /// gives an author for saying it.
    ///
    /// A MISPLACED HEADER STILL NAMES THIS ENTRANCE. It is a defect IN a query
    /// sequence, not a reason to read the file as something else: a submission
    /// that says which world it is in has said so even when it said it in the
    /// wrong place, and reading it as an interactive prompt would replace the
    /// author's placement error with an unrelated syntax error. Only genuinely
    /// unmarked text is one interactive submission and takes the prompt wrap.
    ///
    /// The three framing states reach exactly two roads here, and the tree
    /// records which one it took — [`SyntaxTree::entrance`] is the answer, so
    /// no caller downstream scans for the header a second time.
    pub fn parse_submission(&mut self, source: &str) -> SyntaxTree {
        match framing(source) {
            Framing::Synthetic => self.parse_prompt(source),
            marked => self.frame_query_sequence(marked, source),
        }
    }

    /// The utility entrance's framing, applied. ONE HEADER, WHOEVER WROTE IT.
    fn frame_query_sequence(&mut self, framing: Framing, source: &str) -> SyntaxTree {
        match framing {
            Framing::Authored => self.parse_with("", source, Root::QuerySequence),
            Framing::Synthetic => {
                self.parse_with(QUERY_SEQUENCE_FRAME, source, Root::QuerySequence)
            }
            // Framed as if it had none: the misplaced header is then ordinary
            // text inside the sequence, so it fails where it stands rather
            // than swallowing the file, and the placement teaching rides
            // beside whatever else the parse found.
            Framing::Misframed(at) => {
                let mut tree = self.parse_with(QUERY_SEQUENCE_FRAME, source, Root::QuerySequence);
                tree.misframed = Some(at);
                tree
            }
        }
    }

    /// One interactive submission. The prompt wraps its input as a top-level
    /// goal, which keeps interactive convenience outside the grammar while the
    /// parser still receives canonical text.
    pub fn parse_prompt(&mut self, submission: &str) -> SyntaxTree {
        self.parse_with(PROMPT_SELECTOR, submission, Root::DefinitionFile)
    }

    /// One companion cell, at the root its COLUMN selects. The selector is
    /// written here so no caller has to know it, and so no reader is ever
    /// tempted to classify a cell by its content.
    pub fn parse_companion_cell(&mut self, column: CompanionColumn, cell: &str) -> SyntaxTree {
        let selector = match column {
            CompanionColumn::Constraint => CONSTRAINT_CELL_SELECTOR,
            CompanionColumn::Default => DEFAULT_CELL_SELECTOR,
        };
        self.parse_with(selector, cell, Root::CompanionCell)
    }

    /// [`Parser::parse_prompt`], under a caller-owned cancellation predicate.
    ///
    /// The predicate is polled at the runtime's cooperative checkpoints with
    /// the parse's progress in AUTHORED bytes; answering `true` cancels. The
    /// checkpoints are reached between parse operations, not inside them — a
    /// runtime defect that loops below them (the diagnosed `stack__iter`
    /// recovery loop does) never polls, so this bounds only the parses that
    /// keep making progress. Hard containment needs a process boundary.
    pub fn parse_prompt_cancellable(
        &mut self,
        submission: &str,
        should_cancel: &mut dyn FnMut(usize) -> bool,
    ) -> CancellableParse {
        self.parse_with_cancellation(PROMPT_SELECTOR, submission, Root::DefinitionFile, should_cancel)
    }

    /// [`Parser::parse_submission`], under a caller-owned cancellation
    /// predicate. Framing follows the exact same law as the uncancellable
    /// entrance: the submission's own bytes name the road.
    pub fn parse_submission_cancellable(
        &mut self,
        source: &str,
        should_cancel: &mut dyn FnMut(usize) -> bool,
    ) -> CancellableParse {
        match framing(source) {
            Framing::Synthetic => self.parse_prompt_cancellable(source, should_cancel),
            Framing::Authored => {
                self.parse_with_cancellation("", source, Root::QuerySequence, should_cancel)
            }
            Framing::Misframed(at) => {
                let mut parse = self.parse_with_cancellation(
                    QUERY_SEQUENCE_FRAME,
                    source,
                    Root::QuerySequence,
                    should_cancel,
                );
                if let CancellableParse::Completed(tree) = &mut parse {
                    tree.misframed = Some(at);
                }
                parse
            }
        }
    }

    /// The cancellable core: [`Parser::parse_with`] through the runtime's
    /// progress-callback option. Coordinates handed to the predicate and
    /// reported on cancellation are AUTHORED bytes — the one prefix
    /// convention holds here too.
    fn parse_with_cancellation(
        &mut self,
        selector: &str,
        source: &str,
        entrance: Root,
        should_cancel: &mut dyn FnMut(usize) -> bool,
    ) -> CancellableParse {
        debug_assert!(
            !selector.contains('\n') || selector.ends_with('\n'),
            "framing must end its line or the author's first byte lands mid-row"
        );
        let parsed = format!("{selector}{source}");
        let selector_len = selector.len();
        let mut last_progress_byte: Option<usize> = None;
        let mut progress = |state: &tree_sitter::ParseState| -> bool {
            let authored = state.current_byte_offset().saturating_sub(selector_len);
            last_progress_byte = Some(authored);
            should_cancel(authored)
        };
        let options = tree_sitter::ParseOptions::new().progress_callback(&mut progress);
        let bytes = parsed.as_bytes();
        let mut read = |offset: usize, _pos: Point| -> &[u8] {
            if offset < bytes.len() {
                &bytes[offset..]
            } else {
                &[]
            }
        };
        let tree = self
            .inner
            .parse_with_options(&mut read, None, Some(options));
        match tree {
            Some(tree) => CancellableParse::Completed(SyntaxTree {
                tree,
                parsed,
                selector_len,
                selector_rows: selector.matches('\n').count(),
                misframed: None,
                entrance,
            }),
            None => {
                // A cancelled parse leaves the runtime mid-flight; without a
                // reset the NEXT parse on this parser resumes that state and
                // answers about the wrong text.
                self.inner.reset();
                CancellableParse::Cancelled {
                    last_progress_byte,
                    entrance,
                }
            }
        }
    }

    /// ONE prefix convention for every entrance. A second offset rule is how
    /// one of them ends up reporting coordinates the author cannot use.
    fn parse_with(&mut self, selector: &str, source: &str, entrance: Root) -> SyntaxTree {
        debug_assert!(
            !selector.contains('\n') || selector.ends_with('\n'),
            "framing must end its line or the author's first byte lands mid-row"
        );
        let parsed = format!("{selector}{source}");
        let tree = self
            .inner
            .parse(parsed.as_bytes(), None)
            .expect("parsing without a cancellation flag or timeout cannot fail");
        SyntaxTree {
            tree,
            parsed,
            selector_len: selector.len(),
            selector_rows: selector.matches('\n').count(),
            misframed: None,
            entrance,
        }
    }
}

/// The OUTERMOST nodes of a kind set, in document order: on each path from the
/// root, the first node whose kind is named, and nothing beneath it.
///
/// What this exists for is a DEFECTIVE tree. A consumer dividing a submission
/// needs the forms recovery proved, and recovery is free to leave them under an
/// unrecognized root — so a division that reads the root's shape reads a
/// recovery decision rather than the author's text. Pruning at the named kinds
/// is what keeps a form nested inside another form from counting as one.
pub fn outermost<'t, 'k>(
    tree: &'t SyntaxTree,
    kinds: &'k [cst::Kind],
) -> impl Iterator<Item = AnyNode<'t>> + use<'t, 'k> {
    let mut cursor = tree.tree.walk();
    let mut done = false;
    std::iter::from_fn(move || loop {
        if done {
            return None;
        }
        let node = cursor.node();
        let found = AnyNode::cast(node)
            .filter(|any| any.typed_kind().is_some_and(|kind| kinds.contains(&kind)));
        // A match ends this path: its interior holds only nested forms.
        if found.is_some() || !cursor.goto_first_child() {
            loop {
                if cursor.goto_next_sibling() {
                    break;
                }
                if !cursor.goto_parent() {
                    done = true;
                    break;
                }
            }
        }
        if found.is_some() {
            return found;
        }
    })
}

/// Depth-first walk over every named node, for consumers that genuinely need
/// generic structure. A semantic decision belongs on a typed accessor instead.
pub fn walk(tree: &SyntaxTree) -> impl Iterator<Item = AnyNode<'_>> {
    let mut cursor = tree.tree.walk();
    let mut done = false;
    std::iter::from_fn(move || loop {
        if done {
            return None;
        }
        let node = cursor.node();
        if cursor.goto_first_child() {
        } else {
            loop {
                if cursor.goto_next_sibling() {
                    break;
                }
                if !cursor.goto_parent() {
                    done = true;
                    break;
                }
            }
        }
        if let Some(any) = AnyNode::cast(node) {
            return Some(any);
        }
    })
}
