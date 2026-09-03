// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// REPL module for interactive DelightQL sessions
pub mod commands;
pub mod completions;
pub mod config;

pub mod info_panel;

pub mod multi_pane_tui;
pub mod name_generator;
pub mod parser_worker;
pub mod worker;

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

use self::commands::{handle_dot_command, is_dot_command, process_query, CommandResult, ReplState};
use self::completions::DotCommandCompleter;
use self::config::ReplParserOperation;
use self::multi_pane_tui::run_multi_pane_tui;
use self::parser_worker::{ParserWorkerController, ProbeOutcome};
use self::worker::WorkerResult;

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

/// Every CONTINUATION ANCHOR in the line, as char positions.
///
/// A continuation anchor is where the text to the left is already a relational
/// expression and a continuation may replace what follows — which is exactly
/// what a reader jumping through a chain wants to land on. The chain's own
/// start is one, and so is every continuation within it.
///
/// The parse crosses the containment worker; a probe that does not answer
/// yields no stops, so the shortcut no-ops instead of freezing the editor.
fn find_stop_points(worker: &ParserWorkerController, line: &str) -> Vec<usize> {
    match worker.probe(ReplParserOperation::ContinuationNavigation, line, None) {
        ProbeOutcome::Answer(WorkerResult::Continuations { byte_offsets }) => byte_offsets
            .iter()
            .map(|&bp| byte_to_char_pos(line, bp))
            .collect(),
        _ => Vec::new(),
    }
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

/// The prompt's readiness, told to whoever holds the other end of the
/// pipe named by `DQL_REPL_READY_FD`: one byte before every read. A
/// replay driver synchronizes on it instead of scraping the prompt.
/// Unset, it is nothing; a failed write is nothing too — the human at
/// a real terminal never needs it.
struct ReadySignal(Option<std::os::fd::RawFd>);

impl ReadySignal {
    fn from_environment() -> Self {
        ReadySignal(
            std::env::var("DQL_REPL_READY_FD")
                .ok()
                .and_then(|v| v.trim().parse::<std::os::fd::RawFd>().ok())
                .filter(|fd| *fd >= 0),
        )
    }

    fn signal(&self) {
        #[cfg(unix)]
        if let Some(fd) = self.0 {
            // SAFETY: one byte from a live buffer to a descriptor the
            // parent handed us; a bad descriptor fails, nothing else.
            let _ = unsafe { libc::write(fd, b"\n".as_ptr().cast(), 1) };
        }
    }
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
struct DeleteToNextContinuationHandler {
    worker: Arc<ParserWorkerController>,
}

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

                let stops = find_stop_points(&self.worker, line);
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
struct DeleteToPrevContinuationHandler {
    worker: Arc<ParserWorkerController>,
}

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

                let stops = find_stop_points(&self.worker, line);
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
    worker: Arc<ParserWorkerController>,
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
                // stays available to the completer. A probe that does not
                // answer falls through too.
                if self::completions::is_well_formed(&self.worker, left) != Some(true) {
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
struct PrevContinuationHandler {
    worker: Arc<ParserWorkerController>,
}

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
                let stops = find_stop_points(&self.worker, line);
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
struct NextContinuationHandler {
    worker: Arc<ParserWorkerController>,
}

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
                let stops = find_stop_points(&self.worker, line);
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
            crate::client::incident::warning("config", crate::client::incident::hierarchy::CONFIG, format!("failed to create the config directory: {e}"));
            return None;
        }

        Some(config_dir.join("history"))
    } else {
        crate::client::incident::warning("config", crate::client::incident::hierarchy::CONFIG, "could not determine the config directory for history".to_string());
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
    .unwrap_or_else(|e| crate::client::incident::warning("terminal", crate::client::incident::hierarchy::TERMINAL, format!("could not set the Ctrl-C handler: {e}")));

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
    repl_state.set_show_meta_output(show_meta, "startup");

    // The parser containment boundary: budgets from the typed config, the
    // raw incident writer, and the highlight configuration the worker must
    // mirror. Replaces the default controller before anything probes.
    let parser_worker = Arc::new(ParserWorkerController::new(
        *repl_state.config().parser_budgets(),
        Arc::clone(repl_state.config().editor_helper_policy()),
        repl_state.repl_db.clone(),
        highlights_path.map(|p| p.to_path_buf()),
    ));
    repl_state.parser_worker = Arc::clone(&parser_worker);

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
    let completer = DotCommandCompleter::new(Arc::clone(&parser_worker));
    let config = rustyline::Config::builder()
        .color_mode(rustyline::ColorMode::Enabled)
        .build();
    let mut rl = Editor::with_config(config).context("Failed to create readline editor")?;
    rl.set_helper(Some(completer));
    // The session record certifies which road the prompt ran: a transcript
    // from a plain road (no tty, TERM unsupported) exercised none of the
    // per-keystroke hooks, and must not pass for one that did.
    if let Some(db) = &repl_state.repl_db {
        use crate::client::context::EditorRoad;
        let road = if rl.is_rich_road() {
            EditorRoad::Rich
        } else {
            EditorRoad::Plain
        };
        db.record_editor_road(road);
    }

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
        worker: Arc::clone(&parser_worker),
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
        let delete_next_handler = DeleteToNextContinuationHandler {
            worker: Arc::clone(&parser_worker),
        };
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
        let delete_prev_handler = DeleteToPrevContinuationHandler {
            worker: Arc::clone(&parser_worker),
        };
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
    let prev_cont_handler_lower = PrevContinuationHandler {
        worker: Arc::clone(&parser_worker),
    };
    rl.bind_sequence(
        KeyEvent::ctrl('b'),
        EventHandler::Conditional(Box::new(prev_cont_handler_lower)),
    );
    let prev_cont_handler_upper = PrevContinuationHandler {
        worker: Arc::clone(&parser_worker),
    };
    rl.bind_sequence(
        KeyEvent::ctrl('B'),
        EventHandler::Conditional(Box::new(prev_cont_handler_upper)),
    );

    let next_cont_handler_lower = NextContinuationHandler {
        worker: Arc::clone(&parser_worker),
    };
    rl.bind_sequence(
        KeyEvent::ctrl('f'),
        EventHandler::Conditional(Box::new(next_cont_handler_lower)),
    );
    let next_cont_handler_upper = NextContinuationHandler {
        worker: Arc::clone(&parser_worker),
    };
    rl.bind_sequence(
        KeyEvent::ctrl('F'),
        EventHandler::Conditional(Box::new(next_cont_handler_upper)),
    );

    // Load history if available
    if let Some(history_path) = get_history_path() {
        if history_path.exists() {
            if let Err(e) = rl.load_history(&history_path) {
                crate::client::incident::warning("config", crate::client::incident::hierarchy::CONFIG, format!("failed to load history: {e}"));
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
    let ready = ReadySignal::from_environment();

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
        let prompt = get_prompt(repl_state.config().sql_mode(), is_continuation);

        // The ready byte: a replay driver holding the other end of the
        // pipe waits for it before typing the next line. Written right
        // before the read, on every road, and nothing else changes.
        ready.signal();

        // Use readline_with_initial if we have a preserved line
        let result = if let Some((before, after)) = preserved_line.take() {
            rl.readline_with_initial(prompt, (&before, &after))
        } else {
            rl.readline(prompt)
        };

        match result {
            Ok(line) => {
                if repl_state.config().multiline() {
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
                        repl_state.prepare_tui_snapshot();
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
                        repl_state.prepare_tui_snapshot();
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
                        repl_state.prepare_tui_snapshot();
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
                crate::client::incident::error("terminal", crate::client::incident::hierarchy::TERMINAL, format!("error reading line: {err}"));
                break;
            }
        }
    }

    // Save history
    if let Some(history_path) = get_history_path() {
        if let Err(e) = rl.save_history(&history_path) {
            crate::client::incident::warning("config", crate::client::incident::hierarchy::CONFIG, format!("failed to save history: {e}"));
        }
    }

    // Close the session while the handle — and core's findings in it —
    // is alive: the interactive road always writes its three files.
    {
        let mut handle = repl_state
            .dql_handle
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        crate::client::exit::finish(Some(&mut **handle), 0);
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

