// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
/// Tab completion support for REPL
///
/// This module contains the rustyline helper implementations for tab completion
/// of dot commands and column names in the REPL.
use rustyline::{Context as RustylineContext, Helper};
use std::sync::Arc;

use super::config::ReplParserOperation;
use super::parser_worker::{ParserWorkerController, ProbeOutcome};
use super::worker::WorkerResult;

/// Context for determining what type of completion to provide
#[derive(Debug, Clone)]
enum CompletionContext {
    /// Line starts with '.', complete dot commands
    DotCommand,
    /// Complete column names from the given tables
    ColumnName { tables: Vec<String> },
    /// Unknown context, no completions
    Unknown,
}

/// Largest char boundary at or below `i`. rustyline keeps its cursor on a
/// boundary, but this runs on every redraw and a panic there takes the REPL
/// down, so it does not rely on that.
fn floor_boundary(s: &str, i: usize) -> usize {
    if i >= s.len() {
        return s.len();
    }
    let mut i = i;
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// Split a line at the cursor into (left-of-cursor, right-of-cursor).
///
/// The left half is what the prompt probes and what Tab meta-izes; the pair is
/// also exactly the shape `Editor::readline_with_initial` wants, so restoring a
/// line after an interrupt puts the cursor back where it was.
pub(crate) fn split_at_cursor(line: &str, pos: usize) -> (&str, &str) {
    let cut = floor_boundary(line, pos);
    (&line[..cut], &line[cut..])
}

/// Does `prefix` stand on its own as a complete DQL expression?
///
/// Answers "if Enter were pressed now, with everything right of the cursor
/// discarded, would this run" — NOT "is the user finished". Well-formedness is
/// not monotonic over prefixes: `users(*)` holds, `users(*) |>` does not,
/// `users(*) |> (id)` holds again.
///
/// The parse runs in the containment worker at the prompt entrance — the
/// road the line will take if Enter is pressed. `None` means the probe did
/// not answer (budget exceeded, the worker unavailable, or the optional
/// helpers disabled); the caller keeps its previous verdict or falls
/// through — a timed-out keystroke never freezes the prompt, and Tab's
/// meta-ize never fires without a verdict.
pub(crate) fn is_well_formed(worker: &ParserWorkerController, prefix: &str) -> Option<bool> {
    let prefix = prefix.trim();
    // Dot-commands are REPL directives, not queries.
    if prefix.is_empty() || prefix.starts_with('.') {
        return Some(false);
    }
    match worker.probe(ReplParserOperation::PromptWellFormed, prefix, None) {
        ProbeOutcome::Answer(WorkerResult::WellFormed { well_formed }) => Some(well_formed),
        _ => None,
    }
}

/// Completer for dot commands and column names in the REPL
#[derive(Clone)]
pub struct DotCommandCompleter {
    commands: Vec<String>,
    /// The containment boundary every speculative parse crosses.
    worker: Arc<ParserWorkerController>,
    /// Well-formedness of the prefix left of the cursor, for the buffer being
    /// drawn. Written by `highlight_char`, read by `highlight_prompt` — which
    /// receives no line buffer of its own. `Cell` because every `Highlighter`
    /// method takes `&self`.
    well_formed: std::cell::Cell<bool>,
    // schema: Option<Arc<dyn Schema>>, // Temporarily disabled
}

impl DotCommandCompleter {
    pub fn new(worker: Arc<ParserWorkerController>) -> Self {
        Self {
            // Projection of the dot-command registry — a hand-list here
            // completed fossils (.debug-last had no dispatch arm) and
            // missed real commands.
            commands: super::commands::dot_command_spellings()
                .map(String::from)
                .collect(),
            worker,
            well_formed: std::cell::Cell::new(false),
            // schema: None, // Temporarily disabled
        }
    }

    /// Detect the context for completion
    fn detect_context(&self, line: &str, pos: usize) -> CompletionContext {
        let text = &line[..pos];

        // Check for dot command
        if text.starts_with('.') {
            return CompletionContext::DotCommand;
        }

        // Check for column name context
        // Pattern 1: "table(*), " - filter condition
        // Pattern 2: "table(col, " - field list
        // Pattern 3: "table(*) |> (" - projection
        // Pattern 4: Multiple tables

        // Simple heuristic: if we have a table name followed by parentheses or pipe
        if let Some(tables) = self.extract_tables_in_scope(text) {
            if !tables.is_empty() {
                return CompletionContext::ColumnName { tables };
            }
        }

        CompletionContext::Unknown
    }

    /// Extract table names from the query that are in scope
    fn extract_tables_in_scope(&self, text: &str) -> Option<Vec<String>> {
        let mut tables = Vec::new();

        // Look for DelightQL patterns:
        // 1. tablename(*) or tablename(columns...)
        // 2. Multiple tables: table1(*), table2(*)
        // 3. After pipe operator, maintain context

        // Handle pipe operator - everything before last pipe is context
        let context_text = if let Some(pipe_pos) = text.rfind("|>") {
            // Get the part before the pipe for table context

            // But work with the full text for finding current position
            &text[..pipe_pos]
        } else {
            text
        };

        // Look for table patterns in the context
        // Match patterns like: word(*) or word(anything)
        // Simple pattern matching without regex (to avoid adding dependency)
        let mut i = 0;
        let bytes = context_text.as_bytes();
        while i < bytes.len() {
            // Look for identifier start
            if (bytes[i] as char).is_alphabetic() || bytes[i] == b'_' {
                let start = i;
                // Scan identifier
                while i < bytes.len() && ((bytes[i] as char).is_alphanumeric() || bytes[i] == b'_')
                {
                    i += 1;
                }
                let table_name = &context_text[start..i];

                // Skip whitespace
                while i < bytes.len() && (bytes[i] as char).is_whitespace() {
                    i += 1;
                }

                // Check for '('
                if i < bytes.len() && bytes[i] == b'(' {
                    // Found a table reference
                    tables.push(table_name.to_string());
                }
            } else {
                i += 1;
            }
        }

        // Remove duplicates while preserving order
        let mut unique_tables = Vec::new();
        for table in tables {
            if !unique_tables.contains(&table) {
                unique_tables.push(table);
            }
        }

        if unique_tables.is_empty() {
            None
        } else {
            Some(unique_tables)
        }
    }

    /// Column completions for the given tables. The helper holds no schema —
    /// `delightql-core`'s public boundary is the handle/session API, and a
    /// completer is not a session — so there are no column names to offer.
    fn get_column_completions(&self, _tables: &[String], _prefix: &str) -> Vec<Pair> {
        Vec::new()
    }
}

impl Completer for DotCommandCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &RustylineContext<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let context = self.detect_context(line, pos);

        match context {
            CompletionContext::DotCommand => {
                // Complete dot commands
                let text = &line[..pos];
                let mut matches = Vec::new();
                for command in &self.commands {
                    if command.starts_with(text) {
                        matches.push(Pair {
                            display: command.clone(),
                            replacement: command.clone(),
                        });
                    }
                }
                Ok((0, matches))
            }
            CompletionContext::ColumnName { tables } => {
                // Find the start of the current word being typed
                let text = &line[..pos];
                let word_start = text
                    .rfind(|c: char| {
                        c.is_whitespace() || c == ',' || c == '(' || c == '[' || c == '>'
                    })
                    .map(|i| i + 1)
                    .unwrap_or(0);

                let prefix = &text[word_start..];
                let completions = self.get_column_completions(&tables, prefix);

                Ok((word_start, completions))
            }
            CompletionContext::Unknown => {
                // No completions available
                Ok((pos, Vec::new()))
            }
        }
    }
}

impl Hinter for DotCommandCompleter {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &RustylineContext<'_>) -> Option<String> {
        None
    }
}

impl Highlighter for DotCommandCompleter {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> std::borrow::Cow<'b, str> {
        // `highlight_char` ran earlier in this same refresh (rustyline
        // edit.rs: refresh_line calls it before refresh, and move_cursor calls
        // it before refreshing) so the stamped verdict is current, not one
        // keystroke stale. This hook gets no line buffer of its own.
        //
        // The substitute MUST keep the display width of `prompt`: `prompt_size`
        // was computed once from the original and drives cursor positioning.
        // rustyline measures with `UnicodeWidthStr::width()`, under which `∂`
        // and `?` are both one column.
        if self.well_formed.get() {
            return std::borrow::Cow::Borrowed(prompt);
        }
        match prompt {
            "∂> " => std::borrow::Cow::Borrowed("?> "),
            // SQL mode is not DQL, and the continuation prompt already means
            // "not finished" — neither is probed, so neither is swapped.
            other => std::borrow::Cow::Borrowed(other),
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> std::borrow::Cow<'h, str> {
        std::borrow::Cow::Borrowed(hint)
    }

    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> std::borrow::Cow<'l, str> {
        #[cfg(feature = "prettify")]
        {
            // Spans come from the containment worker; only the ANSI
            // rendering happens here. A probe that does not answer draws
            // the line uncolored — never a frozen prompt.
            match self
                .worker
                .probe(ReplParserOperation::SyntaxHighlight, line, None)
            {
                ProbeOutcome::Answer(WorkerResult::Highlights { spans }) => {
                    crate::repl::syntax_highlighter::render_line(line, &spans)
                }
                _ => std::borrow::Cow::Borrowed(line),
            }
        }
        #[cfg(not(feature = "prettify"))]
        {
            // No syntax highlighting when prettify feature is disabled
            std::borrow::Cow::Borrowed(line)
        }
    }

    fn highlight_char(&self, line: &str, pos: usize) -> bool {
        // With the optional helpers off, the prompt is the ORDINARY prompt:
        // the neutral verdict clears any stale `?>` left from before the
        // breaker opened, and nothing contacts or spawns the worker. A read
        // of the shared policy, not a probe.
        if !self.worker.helpers_enabled() {
            self.well_formed.set(true);
            return true;
        }
        // The only Highlighter hook that receives BOTH the line and the cursor,
        // and rustyline calls it before drawing the prompt on every edit AND
        // every cursor move. So it is where the verdict is computed.
        //
        // Probing `line[..pos]` rather than the whole line is what makes the
        // cursor an evaluation point: scrub left through `users(*) |> (id)` and
        // the prompt flips at each boundary where the prefix stops standing
        // alone.
        let (left, _right) = split_at_cursor(line, pos);
        if let Some(verdict) = is_well_formed(&self.worker, left) {
            self.well_formed.set(verdict);
        }
        // On no-answer the PREVIOUS verdict stands: a timed-out probe keeps
        // the prompt's last honest state instead of flapping or freezing.

        // Returning `true` forces a full refresh, which is what redraws the
        // prompt. Returning `false` lets rustyline take its fast paths — write
        // the character directly, or move the terminal cursor — and the prompt
        // would then freeze mid-expression. Syntax highlighting needs the
        // refresh on edits anyway; the prompt needs it on cursor moves too.
        true
    }

    fn highlight_candidate<'c>(
        &self,
        candidate: &'c str,
        _completion: rustyline::CompletionType,
    ) -> std::borrow::Cow<'c, str> {
        std::borrow::Cow::Borrowed(candidate)
    }
}

impl Validator for DotCommandCompleter {
    fn validate(&self, _ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        Ok(ValidationResult::Valid(None))
    }

    fn validate_while_typing(&self) -> bool {
        false
    }
}

impl Helper for DotCommandCompleter {}

// End of temporarily disabled test module
