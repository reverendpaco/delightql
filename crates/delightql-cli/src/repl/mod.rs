// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// REPL module for interactive DelightQL sessions
pub mod commands;
pub mod completions;
pub mod info_panel;
pub mod multi_pane_tui;
pub mod name_generator;

#[cfg(feature = "prettify")]
pub mod syntax_highlighter;

use anyhow::{Context, Result};
use rustyline::{
    Cmd, ConditionalEventHandler, Editor, Event, EventContext, EventHandler, KeyCode, KeyEvent,
    Modifiers,
};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::output_format::OutputFormat;
use std::sync::atomic::{AtomicBool, Ordering};
use delightql_cst::cst::{self, TypedNode};
use delightql_cst::{Parser, SyntaxTree};

use self::commands::{handle_dot_command, is_dot_command, process_query, CommandResult, ReplState};
use self::completions::DotCommandCompleter;
use self::multi_pane_tui::run_multi_pane_tui;

/// The DelightQL language, for the highlighting substrate.
///
/// `tree-sitter-highlight` is grammar-agnostic and takes a raw `Language`; it
/// is the one place a runtime handle is still the right currency. Everything
/// else in the prompt reads the typed CST.
///
/// Lives here rather than in `syntax_highlighter` because that module is behind
/// the optional `prettify` feature while the prompt's well-formedness probe is
/// not — `--no-default-features --features repl` must still build.
pub(crate) fn dql_language() -> tree_sitter::Language {
    delightql_cst::language()
}

/// Read one prompt line.
///
/// The REPL's line is an interactive submission, so it takes the same entrance
/// the compiler gives one: the prompt wrap. Reading it any other way would let
/// the prompt disagree with the road that will actually run the line.
fn parse_line(line: &str) -> SyntaxTree {
    Parser::new().parse_prompt(line)
}

/// Every CONTINUATION ANCHOR in the line, as char positions.
///
/// A continuation anchor is where the text to the left is already a relational
/// expression and a continuation may replace what follows — which is exactly
/// what a reader jumping through a chain wants to land on. The chain's own
/// start is one, and so is every continuation within it.
fn find_stop_points(line: &str) -> Vec<usize> {
    let tree = parse_line(line);
    let mut byte_positions: Vec<usize> = delightql_cst::walk(&tree)
        .filter(|node| {
            // `Continuation` is a supertype, so the cast is how its whole
            // family is named at once — a member added to the grammar becomes
            // an anchor here without this line changing.
            cst::Continuation::cast(node.node()).is_some()
                || cst::Relex::cast(node.node()).is_some()
                || cst::Effrelex::cast(node.node()).is_some()
        })
        .filter_map(|node| tree.byte_range(node).map(|range| range.start))
        .collect();
    byte_positions.sort_unstable();
    byte_positions.dedup();
    byte_positions
        .iter()
        .map(|&bp| byte_to_char_pos(line, bp))
        .collect()
}

/// Convert byte position to character position
fn byte_to_char_pos(s: &str, byte_pos: usize) -> usize {
    s.char_indices().take_while(|(i, _)| *i < byte_pos).count()
}

/// Check if the terminal supports Unicode
fn supports_unicode() -> bool {
    // Check environment variables for UTF-8 support
    if let Ok(lang) = std::env::var("LANG") {
        if lang.contains("UTF-8") || lang.contains("utf8") || lang.contains("utf-8") {
            return true;
        }
    }
    if let Ok(lc_all) = std::env::var("LC_ALL") {
        if lc_all.contains("UTF-8") || lc_all.contains("utf8") || lc_all.contains("utf-8") {
            return true;
        }
    }
    if let Ok(lc_ctype) = std::env::var("LC_CTYPE") {
        if lc_ctype.contains("UTF-8") || lc_ctype.contains("utf8") || lc_ctype.contains("utf-8") {
            return true;
        }
    }

    // Check for known Unicode-supporting terminals on Windows
    #[cfg(windows)]
    {
        // Windows Terminal or ConEmu support Unicode
        if std::env::var("WT_SESSION").is_ok() || std::env::var("ConEmuPID").is_ok() {
            return true;
        }
    }

    // Check terminal type - if it's not "dumb" or empty, assume basic Unicode support
    if let Ok(term) = std::env::var("TERM") {
        return !term.is_empty() && term != "dumb" && term != "vt100";
    }

    false
}

/// Get the appropriate prompt based on mode and Unicode support
fn get_prompt(sql_mode: bool, is_continuation: bool) -> &'static str {
    let supports_unicode = supports_unicode();

    if is_continuation {
        "  -> " // Continuation prompt (always ASCII)
    } else if sql_mode {
        "SQL> "
    } else if supports_unicode {
        "∂> " // Delta prompt for DelightQL
    } else {
        "> " // ASCII fallback
    }
}

/// Custom event handler for Ctrl+X, t to toggle multi-pane TUI
struct MultiPaneTuiToggleHandler {
    trigger_multi_pane_tui: Arc<Mutex<bool>>,
    current_line: Arc<Mutex<String>>,
}

impl ConditionalEventHandler for MultiPaneTuiToggleHandler {
    fn handle(
        &self,
        evt: &Event,
        _: rustyline::RepeatCount,
        _: bool,
        ctx: &EventContext,
    ) -> Option<Cmd> {
        // Check for Ctrl+X, t sequence
        if let (Some(k1), Some(k2)) = (evt.get(0), evt.get(1)) {
            if (*k1 == KeyEvent::ctrl('X') || *k1 == KeyEvent::ctrl('x'))
                && (*k2 == KeyEvent(KeyCode::Char('t'), Modifiers::NONE)
                    || *k2 == KeyEvent(KeyCode::Char('T'), Modifiers::SHIFT))
            {
                // Save current line content
                if let Ok(mut line) = self.current_line.lock() {
                    *line = ctx.line().to_string();
                }
                // Set flag to trigger multi-pane TUI
                if let Ok(mut trigger) = self.trigger_multi_pane_tui.lock() {
                    *trigger = true;
                }
                // Use Interrupt to break out of readline
                return Some(Cmd::Interrupt);
            }
        }
        None
    }
}

/// Custom event handler for Ctrl+X, d to delete to next continuation
struct DeleteToNextContinuationHandler;

impl ConditionalEventHandler for DeleteToNextContinuationHandler {
    fn handle(
        &self,
        evt: &Event,
        _: rustyline::RepeatCount,
        _: bool,
        ctx: &EventContext,
    ) -> Option<Cmd> {
        // Check for Ctrl+X, d sequence
        if let (Some(k1), Some(k2)) = (evt.get(0), evt.get(1)) {
            if (*k1 == KeyEvent::ctrl('X') || *k1 == KeyEvent::ctrl('x'))
                && *k2 == KeyEvent(KeyCode::Char('d'), Modifiers::NONE)
            {
                let current_pos = ctx.pos();
                let line = ctx.line();

                let stops = find_stop_points(line);
                let target = stops
                    .iter()
                    .find(|&&p| p > current_pos)
                    .copied()
                    .unwrap_or_else(|| line.chars().count());
                if target > current_pos {
                    return Some(Cmd::Replace(
                        rustyline::Movement::ForwardChar(target - current_pos),
                        None,
                    ));
                }
                return Some(Cmd::Noop);
            }
        }
        None
    }
}

/// Custom event handler for Ctrl+X, D to delete to previous continuation
struct DeleteToPrevContinuationHandler;

impl ConditionalEventHandler for DeleteToPrevContinuationHandler {
    fn handle(
        &self,
        evt: &Event,
        _: rustyline::RepeatCount,
        _: bool,
        ctx: &EventContext,
    ) -> Option<Cmd> {
        // Check for Ctrl+X, D sequence
        if let (Some(k1), Some(k2)) = (evt.get(0), evt.get(1)) {
            if (*k1 == KeyEvent::ctrl('X') || *k1 == KeyEvent::ctrl('x'))
                && *k2 == KeyEvent(KeyCode::Char('D'), Modifiers::SHIFT)
            {
                let current_pos = ctx.pos();
                let line = ctx.line();

                let stops = find_stop_points(line);
                let target = stops
                    .iter()
                    .rev()
                    .find(|&&p| p < current_pos)
                    .copied()
                    .unwrap_or(0);
                if current_pos > target {
                    return Some(Cmd::Replace(
                        rustyline::Movement::BackwardChar(current_pos - target),
                        None,
                    ));
                }
                return Some(Cmd::Noop);
            }
        }
        None
    }
}

/// Custom event handler for Alt+Enter to insert newline instead of submit
struct MultiLineHandler;

impl ConditionalEventHandler for MultiLineHandler {
    fn handle(
        &self,
        evt: &Event,
        _: rustyline::RepeatCount,
        _: bool,
        _ctx: &EventContext,
    ) -> Option<Cmd> {
        if let Some(k) = evt.get(0) {
            // Check for Alt+Enter (Option+Enter on Mac)
            if *k == KeyEvent::alt('\r') || *k == KeyEvent::alt('\n') {
                // Insert a newline instead of accepting the line
                Some(Cmd::Newline)
            } else {
                None
            }
        } else {
            None
        }
    }
}

/// Custom event handler for Tab to display schema (META-IZE) of current expression
///
/// Meta-izes the prefix LEFT OF THE CURSOR, which is the same span the prompt
/// reports well-formedness for. The prompt says whether that prefix runs; Tab
/// says what it publishes. Tab on a prefix the prompt marks `?>` falls through
/// rather than interrupting the line to print a parse error.
struct SchemaDisplayHandler {
    trigger_schema_display: Arc<Mutex<bool>>,
    current_line: Arc<Mutex<String>>,
    /// Cursor byte offset at the moment Tab was pressed, so the span meta-ized
    /// and the cursor restored afterwards are both the ones the user saw.
    schema_cursor: Arc<Mutex<usize>>,
}

impl ConditionalEventHandler for SchemaDisplayHandler {
    fn handle(
        &self,
        evt: &Event,
        _: rustyline::RepeatCount,
        _: bool,
        ctx: &EventContext,
    ) -> Option<Cmd> {
        if let Some(k) = evt.get(0) {
            if *k == KeyEvent(KeyCode::Tab, Modifiers::NONE) {
                let line = ctx.line();

                // Fall through to dot-command completer for dot commands
                if line.starts_with('.') {
                    return None;
                }

                let (left, _right) = self::completions::split_at_cursor(line, ctx.pos());

                // Meta-izing a prefix that does not parse builds `<junk> ^` and
                // prints a parse error, having already torn down the line to do
                // it. Fall through instead: no interrupt, no error, and Tab
                // stays available to the completer.
                if !self::completions::is_well_formed(left) {
                    return None;
                }

                // Save the line AND the cursor: the trigger block meta-izes the
                // prefix and restores the cursor to this offset.
                if let Ok(mut stored) = self.current_line.lock() {
                    *stored = line.to_string();
                }
                if let Ok(mut at) = self.schema_cursor.lock() {
                    *at = ctx.pos();
                }
                if let Ok(mut trigger) = self.trigger_schema_display.lock() {
                    *trigger = true;
                }
                return Some(Cmd::Interrupt);
            }
        }
        None
    }
}

/// Custom event handler for Ctrl-B to jump to previous relational continuation
struct PrevContinuationHandler;

impl ConditionalEventHandler for PrevContinuationHandler {
    fn handle(
        &self,
        evt: &Event,
        _n: rustyline::RepeatCount,
        _: bool,
        ctx: &EventContext,
    ) -> Option<Cmd> {
        if let Some(k) = evt.get(0) {
            if *k == KeyEvent::ctrl('b') || *k == KeyEvent::ctrl('B') {
                let current_pos = ctx.pos();
                let line = ctx.line();
                let stops = find_stop_points(line);
                if stops.is_empty() {
                    return Some(Cmd::Noop);
                }
                if let Some(&target) = stops.iter().rev().find(|&&p| p < current_pos) {
                    return Some(Cmd::Move(rustyline::Movement::BackwardChar(
                        current_pos - target,
                    )));
                }
                // Wrap to last stop
                if let Some(&target) = stops.last() {
                    if target > current_pos {
                        return Some(Cmd::Move(rustyline::Movement::ForwardChar(
                            target - current_pos,
                        )));
                    } else if target < current_pos {
                        return Some(Cmd::Move(rustyline::Movement::BackwardChar(
                            current_pos - target,
                        )));
                    }
                }
                return Some(Cmd::Noop);
            }
        }
        None
    }
}

/// Custom event handler for Ctrl-F to jump to next relational continuation
struct NextContinuationHandler;

impl ConditionalEventHandler for NextContinuationHandler {
    fn handle(
        &self,
        evt: &Event,
        _n: rustyline::RepeatCount,
        _: bool,
        ctx: &EventContext,
    ) -> Option<Cmd> {
        if let Some(k) = evt.get(0) {
            if *k == KeyEvent::ctrl('f') || *k == KeyEvent::ctrl('F') {
                let current_pos = ctx.pos();
                let line = ctx.line();
                let stops = find_stop_points(line);
                if stops.is_empty() {
                    return Some(Cmd::Noop);
                }
                if let Some(&target) = stops.iter().find(|&&p| p > current_pos) {
                    return Some(Cmd::Move(rustyline::Movement::ForwardChar(
                        target - current_pos,
                    )));
                }
                // Wrap to first stop
                if let Some(&target) = stops.first() {
                    if target < current_pos {
                        return Some(Cmd::Move(rustyline::Movement::BackwardChar(
                            current_pos - target,
                        )));
                    } else if target > current_pos {
                        return Some(Cmd::Move(rustyline::Movement::ForwardChar(
                            target - current_pos,
                        )));
                    }
                }
                return Some(Cmd::Noop);
            }
        }
        None
    }
}

/// Get the path to the history file, creating config directory if needed
fn get_history_path() -> Option<PathBuf> {
    // Check if history is disabled via environment variable
    if let Ok(val) = std::env::var("DELIGHTQL_NO_HISTORY") {
        if val == "1" || val.to_lowercase() == "true" || val.to_lowercase() == "yes" {
            return None;
        }
    }

    // Get standard config directory
    if let Some(proj_dirs) = directories::ProjectDirs::from("", "", "delightql") {
        let config_dir = proj_dirs.config_dir();

        // Try to create config directory if it doesn't exist
        if let Err(e) = fs::create_dir_all(config_dir) {
            eprintln!("Warning: Failed to create config directory: {}", e);
            return None;
        }

        Some(config_dir.join("history"))
    } else {
        eprintln!("Warning: Could not determine config directory for history");
        None
    }
}

// Global flag for query interruption
static QUERY_INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// Run the interactive REPL
pub fn run_interactive(
    db_path: Option<String>,
    output_format: OutputFormat,
    quiet: bool,
    highlights_path: Option<&std::path::Path>,
) -> Result<()> {
    run_interactive_with_connection(db_path, output_format, quiet, highlights_path, None)
}

/// Run the interactive REPL with an optional existing connection
pub fn run_interactive_with_connection(
    db_path: Option<String>,
    output_format: OutputFormat,
    quiet: bool,
    highlights_path: Option<&std::path::Path>,
    connection: Option<crate::connection::ConnectionManager>,
) -> Result<()> {
    // Set up Ctrl-C handler once at the beginning
    ctrlc::set_handler(|| {
        QUERY_INTERRUPTED.store(true, Ordering::Relaxed);
    })
    .unwrap_or_else(|e| eprintln!("Warning: Could not set Ctrl-C handler: {}", e));

    // Initialize syntax highlighter with config (if prettify feature enabled)
    #[cfg(feature = "prettify")]
    {
        let highlight_config = syntax_highlighter::HighlightConfig::from_path(highlights_path);
        syntax_highlighter::init_highlighter(highlight_config);
    }

    // In interactive mode, default is verbose unless quiet is specified
    let show_meta = !quiet;

    if show_meta {
        println!("DelightQL REPL - Interactive Mode");
        println!("Type '.help' for commands, '.exit' to quit");
        println!("Use Alt+Enter for multi-line queries");
        println!("Use Ctrl-B/Ctrl-F to jump between continuations");
        println!("Use Ctrl-X, t to toggle TUI");
        println!("Use Ctrl-X, d/D to delete to next/prev continuation");
        if highlights_path.is_some() {
            println!("Using custom syntax highlighting");
        }
    }

    // Create REPL state (with optional connection)
    let mut repl_state =
        ReplState::new_with_connection(db_path.clone(), output_format, connection)?;
    repl_state.show_meta_output = show_meta;

    // Show database type if in verbose mode
    if show_meta {
        let db_type = repl_state.db_connection.database_type();
        let db_location = db_path
            .as_ref()
            .map(|p| format!("file: {}", p))
            .unwrap_or_else(|| "memory".to_string());
        println!("Connected to {} ({})", db_type, db_location);
    }

    // Set up readline editor with completion
    let completer = DotCommandCompleter::new();
    let config = rustyline::Config::builder()
        .color_mode(rustyline::ColorMode::Enabled)
        .build();
    let mut rl = Editor::with_config(config).context("Failed to create readline editor")?;
    rl.set_helper(Some(completer));

    // Use Ctrl+X as leader key (avoids conflicts with Ctrl+T transpose)
    // Add custom event handler for Ctrl+X, t (toggle TUI)
    let trigger_multi_pane_tui = Arc::new(Mutex::new(false));
    let current_line_storage = Arc::new(Mutex::new(String::new()));

    for ctrl_x_key in [KeyEvent::ctrl('x'), KeyEvent::ctrl('X')] {
        for t_key in [
            KeyEvent(KeyCode::Char('t'), Modifiers::NONE),
            KeyEvent(KeyCode::Char('T'), Modifiers::SHIFT),
        ] {
            let tui_handler = MultiPaneTuiToggleHandler {
                trigger_multi_pane_tui: trigger_multi_pane_tui.clone(),
                current_line: current_line_storage.clone(),
            };
            rl.bind_sequence(
                Event::KeySeq(vec![ctrl_x_key, t_key]),
                EventHandler::Conditional(Box::new(tui_handler)),
            );
        }
    }

    // Add custom event handler for Tab (schema display via META-IZE)
    let trigger_schema_display = Arc::new(Mutex::new(false));
    let schema_cursor = Arc::new(Mutex::new(0usize));
    let schema_handler = SchemaDisplayHandler {
        trigger_schema_display: trigger_schema_display.clone(),
        current_line: current_line_storage.clone(),
        schema_cursor: schema_cursor.clone(),
    };
    rl.bind_sequence(
        KeyEvent(KeyCode::Tab, Modifiers::NONE),
        EventHandler::Conditional(Box::new(schema_handler)),
    );

    // Add custom event handler for Ctrl+X, d (delete to next continuation)
    for ctrl_x_key in [KeyEvent::ctrl('x'), KeyEvent::ctrl('X')] {
        let delete_next_handler = DeleteToNextContinuationHandler;
        rl.bind_sequence(
            Event::KeySeq(vec![
                ctrl_x_key,
                KeyEvent(KeyCode::Char('d'), Modifiers::NONE),
            ]),
            EventHandler::Conditional(Box::new(delete_next_handler)),
        );
    }

    // Add custom event handler for Ctrl+X, D (delete to previous continuation)
    for ctrl_x_key in [KeyEvent::ctrl('x'), KeyEvent::ctrl('X')] {
        let delete_prev_handler = DeleteToPrevContinuationHandler;
        rl.bind_sequence(
            Event::KeySeq(vec![
                ctrl_x_key,
                KeyEvent(KeyCode::Char('D'), Modifiers::SHIFT),
            ]),
            EventHandler::Conditional(Box::new(delete_prev_handler)),
        );
    }

    // Add custom event handler for Alt+Enter to insert newline
    let multiline_handler = MultiLineHandler;
    rl.bind_sequence(
        KeyEvent::alt('\r'),
        EventHandler::Conditional(Box::new(multiline_handler)),
    );

    // Add custom event handlers for Ctrl-B / Ctrl-F to navigate continuations
    // Note: Ctrl keys can come through as either uppercase or lowercase
    let prev_cont_handler_lower = PrevContinuationHandler;
    rl.bind_sequence(
        KeyEvent::ctrl('b'),
        EventHandler::Conditional(Box::new(prev_cont_handler_lower)),
    );
    let prev_cont_handler_upper = PrevContinuationHandler;
    rl.bind_sequence(
        KeyEvent::ctrl('B'),
        EventHandler::Conditional(Box::new(prev_cont_handler_upper)),
    );

    let next_cont_handler_lower = NextContinuationHandler;
    rl.bind_sequence(
        KeyEvent::ctrl('f'),
        EventHandler::Conditional(Box::new(next_cont_handler_lower)),
    );
    let next_cont_handler_upper = NextContinuationHandler;
    rl.bind_sequence(
        KeyEvent::ctrl('F'),
        EventHandler::Conditional(Box::new(next_cont_handler_upper)),
    );

    // Load history if available
    if let Some(history_path) = get_history_path() {
        if history_path.exists() {
            if let Err(e) = rl.load_history(&history_path) {
                eprintln!("Warning: Failed to load history: {}", e);
            }
        }
    }

    // Main REPL loop.
    //
    // A restored line carries its cursor: `(before, after)` is exactly
    // `readline_with_initial`'s tuple, so an interrupt-and-restore round trip
    // (Tab's meta-ize, the TUI toggle) puts the caret back where the user left
    // it. Storing only the text forces it to end-of-line.
    let mut preserved_line: Option<(String, String)> = None;
    let mut multiline_buffer: Vec<String> = vec![];

    loop {
        // A REPL prompt is a recovery boundary: never present an ordinary
        // DQL prompt backed by a quarantined session. The check reads the
        // typed health report; a failed recovery is a terminal connection
        // failure, not another prompt.
        if let CommandResult::Exit = commands::prompt_recovery_boundary(&mut repl_state) {
            break;
        }

        // Show continuation prompt when buffer has content or preserved line has newlines
        let is_continuation = !multiline_buffer.is_empty()
            || preserved_line
                .as_ref()
                .map_or(false, |(l, r)| l.contains('\n') || r.contains('\n'));
        let prompt = get_prompt(repl_state.sql_mode, is_continuation);

        // Use readline_with_initial if we have a preserved line
        let result = if let Some((before, after)) = preserved_line.take() {
            rl.readline_with_initial(prompt, (&before, &after))
        } else {
            rl.readline(prompt)
        };

        match result {
            Ok(line) => {
                if repl_state.multiline {
                    let trimmed = line.trim();

                    if trimmed.is_empty() {
                        if multiline_buffer.is_empty() {
                            // Nothing buffered, nothing to submit — skip
                            continue;
                        }
                        // Submit the accumulated buffer
                        let full_query = multiline_buffer.join("\n");
                        multiline_buffer.clear();

                        // Add the full multi-line query as a single history entry
                        let _ = rl.add_history_entry(&full_query);

                        match process_input(full_query.trim(), &mut repl_state, &QUERY_INTERRUPTED)
                        {
                            Ok(CommandResult::Continue) => continue,
                            Ok(CommandResult::Exit) => break,
                            Err(e) => {
                                eprintln!("Error: {}", e);
                            }
                        }
                    } else if multiline_buffer.is_empty() && trimmed == "." {
                        // Single dot toggles multi-pane TUI — only when buffer empty
                        repl_state.sync_shared_config();
                        let handle = repl_state.dql_handle.clone();
                        let connection = repl_state.db_connection.clone();
                        let final_window_position =
                            run_multi_pane_tui(repl_state.shared_info.clone(), handle, connection)?;
                        repl_state.shared_info.last_window_position = Some(final_window_position);
                        continue;
                    } else if multiline_buffer.is_empty() && is_dot_command(trimmed) {
                        // Dot commands execute immediately when buffer is empty
                        let _ = rl.add_history_entry(&line);
                        match handle_dot_command(trimmed, &mut repl_state) {
                            Ok(CommandResult::Continue) => continue,
                            Ok(CommandResult::Exit) => break,
                            Err(e) => {
                                eprintln!("Error: {}", e);
                            }
                        }
                    } else {
                        // Accumulate into buffer
                        multiline_buffer.push(line.clone());
                        continue;
                    }
                } else {
                    // Multiline off — original behavior
                    if line.chars().all(|c| c.is_whitespace()) && !line.contains('\n') {
                        continue;
                    }

                    let line_to_process = line.trim();

                    // Special case: single dot toggles multi-pane TUI
                    if line_to_process == "." {
                        repl_state.sync_shared_config();
                        let handle = repl_state.dql_handle.clone();
                        let connection = repl_state.db_connection.clone();
                        let final_window_position =
                            run_multi_pane_tui(repl_state.shared_info.clone(), handle, connection)?;
                        repl_state.shared_info.last_window_position = Some(final_window_position);
                        continue;
                    }

                    let _ = rl.add_history_entry(&line);

                    match process_input(line_to_process, &mut repl_state, &QUERY_INTERRUPTED) {
                        Ok(CommandResult::Continue) => continue,
                        Ok(CommandResult::Exit) => break,
                        Err(e) => {
                            eprintln!("Error: {}", e);
                        }
                    }
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                // Check if this was triggered by Ctrl+T
                if let Ok(trigger) = trigger_multi_pane_tui.lock() {
                    if *trigger {
                        // Reset the trigger
                        drop(trigger);
                        if let Ok(mut trigger) = trigger_multi_pane_tui.lock() {
                            *trigger = false;
                        }

                        // Get the saved line
                        let saved_line = if let Ok(line) = current_line_storage.lock() {
                            line.clone()
                        } else {
                            String::new()
                        };

                        // Update shared info with the current line
                        repl_state.shared_info.last_input = saved_line.clone();

                        // Open multi-pane TUI
                        repl_state.sync_shared_config();
                        let handle = repl_state.dql_handle.clone();
                        let connection = repl_state.db_connection.clone();
                        let final_window_position =
                            run_multi_pane_tui(repl_state.shared_info.clone(), handle, connection)?;
                        repl_state.shared_info.last_window_position = Some(final_window_position);

                        // Preserve the line for the next iteration. The TUI
                        // toggle records no cursor, so it restores at end.
                        preserved_line = Some((saved_line, String::new()));
                        continue;
                    }
                }

                // Check if this was triggered by Tab (schema display)
                if let Ok(trigger) = trigger_schema_display.lock() {
                    if *trigger {
                        drop(trigger);
                        if let Ok(mut trigger) = trigger_schema_display.lock() {
                            *trigger = false;
                        }

                        let saved_line = if let Ok(line) = current_line_storage.lock() {
                            line.clone()
                        } else {
                            String::new()
                        };

                        let at = schema_cursor.lock().map(|g| *g).unwrap_or(saved_line.len());
                        let (left, right) = self::completions::split_at_cursor(&saved_line, at);

                        // Meta-ize the prefix the cursor stood after — the same
                        // span the prompt was reporting on. The handler already
                        // established it is well-formed.
                        let schema_query = format!("{} ^", left.trim());
                        let _ = process_input(&schema_query, &mut repl_state, &QUERY_INTERRUPTED);

                        // Restore with the cursor where it was. `(left, right)`
                        // is readline_with_initial's (before-cursor,
                        // after-cursor); passing "" on the right — as this did —
                        // silently jumps the cursor to end of line.
                        preserved_line = Some((left.to_string(), right.to_string()));
                        continue;
                    }
                }

                if !multiline_buffer.is_empty() {
                    // Discard partial multiline input
                    multiline_buffer.clear();
                    println!();
                } else {
                    println!("CTRL+C");
                }
                continue;
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                println!("CTRL+D - Exiting");
                break;
            }
            Err(err) => {
                eprintln!("Error reading line: {}", err);
                break;
            }
        }
    }

    // Save history
    if let Some(history_path) = get_history_path() {
        if let Err(e) = rl.save_history(&history_path) {
            eprintln!("Warning: Failed to save history: {}", e);
        }
    }

    println!("Goodbye!");
    Ok(())
}

/// Process a line of input (dot command or query)
fn process_input(
    line: &str,
    repl_state: &mut ReplState,
    interrupted_flag: &AtomicBool,
) -> Result<CommandResult> {
    if is_dot_command(line) {
        handle_dot_command(line, repl_state)
    } else {
        process_query(line, repl_state, interrupted_flag)?;
        Ok(CommandResult::Continue)
    }
}

/// Process input with interactive commands (handles dot commands in piped/file input)
fn process_interactive_input(
    input: &str,
    db_path: Option<String>,
    output_format: OutputFormat,
    target_stage: Option<crate::args::Stage>,
    show_meta: bool,
    no_headers: bool,
) -> Result<()> {
    let mut repl_state = commands::ReplState::new(db_path.clone(), output_format)?;
    repl_state.target_stage = target_stage;
    repl_state.show_meta_output = show_meta;
    repl_state.no_headers = no_headers;

    let mut query_buffer = String::new();

    // Process each line
    for line in input.lines() {
        let trimmed = line.trim();

        // Skip empty lines between queries
        if trimmed.is_empty() && query_buffer.is_empty() {
            continue;
        }

        // Check if it's a dot command
        if commands::is_dot_command(trimmed) {
            // Execute any buffered query first
            if !query_buffer.is_empty() {
                // Create a dummy flag for non-interactive mode
                let dummy_flag = std::sync::atomic::AtomicBool::new(false);
                commands::process_query(&query_buffer, &mut repl_state, &dummy_flag)?;
                query_buffer.clear();
            }

            // Handle the dot command
            match commands::handle_dot_command(trimmed, &mut repl_state)? {
                commands::CommandResult::Exit => {
                    // Exit command encountered, stop processing
                    break;
                }
                commands::CommandResult::Continue => {
                    // Continue to next line
                    continue;
                }
            }
        } else if !trimmed.is_empty() {
            // Add to query buffer (queries continue until a dot command or empty line)
            if !query_buffer.is_empty() {
                query_buffer.push(' ');
            }
            query_buffer.push_str(trimmed);
        } else if !query_buffer.is_empty() {
            // Empty line with buffered query - execute it
            let dummy_flag = std::sync::atomic::AtomicBool::new(false);
            commands::process_query(&query_buffer, &mut repl_state, &dummy_flag)?;
            query_buffer.clear();
        }
    }

    // Execute any remaining buffered query
    if !query_buffer.is_empty() {
        let dummy_flag = std::sync::atomic::AtomicBool::new(false);
        commands::process_query(&query_buffer, &mut repl_state, &dummy_flag)?;
    }

    Ok(())
}

/// Process piped input (non-interactive)
pub fn process_piped_input(
    input: &str,
    db_path: Option<String>,
    output_format: OutputFormat,
    target_stage: Option<crate::args::Stage>,
    interactive: bool,
    quiet: bool,
    verbose: bool,
    no_headers: bool,
    no_sanitize: bool,
    connection: Option<crate::connection::ConnectionManager>,
) -> Result<()> {
    if interactive {
        let show_meta = verbose && !quiet;
        process_interactive_input(
            input,
            db_path,
            output_format,
            target_stage,
            show_meta,
            no_headers,
        )
    } else {
        let _conn = connection
            .ok_or_else(|| anyhow::anyhow!("No database connection available for piped input"))?;

        let mut handle = crate::connection::open_handle()?;

        let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;

        // mount! the user database as "main" if specified
        if let Some(ref path) = db_path {
            crate::exec_ng::run_dql_query(
                &format!("mount!(\"{}\", \"main\")(*)", path),
                &mut *session,
            )?;
        }

        crate::exec_ng::execute_query(
            input,
            &mut *session,
            target_stage,
            output_format,
            no_headers,
            no_sanitize,
            false,
        )?;
        Ok(())
    }
}
