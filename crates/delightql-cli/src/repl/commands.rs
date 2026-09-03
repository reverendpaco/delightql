// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// Shared REPL command handler
use anyhow::Result;
use std::time::Instant;

use super::info_panel::SharedReplState;
use crate::args::Stage;
use crate::connection::ConnectionManager;
use crate::output_format::OutputFormat;
use delightql_backends::SqliteExecutor;
use std::sync::Arc;

/// A captured REPL query, stored as a named view in the repl namespace.
pub struct ReplCapture {
    pub name: String,
    pub query_text: String,
    pub seq: u32,
    pub captured_at: String,
}

/// State maintained across REPL interactions
pub struct ReplState {
    pub db_path: Option<String>,
    pub last_query: Option<String>,
    pub last_execution_time: Option<std::time::Duration>,
    pub shared_info: SharedReplState, // Shared with multi-pane TUI
    pub db_connection: ConnectionManager, // Persistent database connection
    pub dql_handle: Arc<std::sync::Mutex<Box<dyn delightql_core::api::DqlHandle>>>, // Persistent DqlHandle (wrapped in Arc<Mutex> for thread-safe mutation)
    /// The typed operational configuration — the one authority. Private to
    /// this module; every mutation crosses the typed operations below, which
    /// also project the option row and refresh the TUI snapshot.
    config: super::config::ReplConfig,
    pub name_generator: super::name_generator::ReplNameGenerator,
    pub captures: Vec<ReplCapture>,
    pub repl_namespace_initialized: bool,
    /// The live client database. `None` only when its construction failed at
    /// startup — the REPL then runs with `repl::*` unavailable, loudly.
    pub repl_db: Option<Arc<crate::client::database::ClientDatabase>>,
    /// Whether the `repl::*` namespace is currently installed over the live
    /// database. Cleared by a Core session reset until the remount succeeds.
    pub repl_namespace_available: bool,
    /// The parser containment boundary. The interactive entry point replaces
    /// this with a highlights-aware controller before any probe runs.
    pub parser_worker: Arc<super::parser_worker::ParserWorkerController>,
}

impl ReplState {
    /// The interactive state over the PROCESS's client database.
    pub fn new(db_path: Option<String>, output_format: OutputFormat) -> Result<Self> {
        Self::new_with_connection(db_path, output_format, None)
    }

    pub fn new_with_connection(
        db_path: Option<String>,
        output_format: OutputFormat,
        connection: Option<ConnectionManager>,
    ) -> Result<Self> {
        Self::new_over(
            db_path,
            output_format,
            connection,
            crate::client::context::process_database(),
        )
    }

    /// The same over a NAMED client database. The state never chooses its
    /// database: production hands it the process's one; a test hands it a
    /// private one so ledgers do not bleed between states in one process.
    pub fn new_over(
        db_path: Option<String>,
        output_format: OutputFormat,
        connection: Option<ConnectionManager>,
        repl_db: Option<Arc<crate::client::database::ClientDatabase>>,
    ) -> Result<Self> {
        // Create DqlHandle via factory-only open
        let db_connection = if let Some(conn) = connection {
            conn
        } else if let Some(ref path) = db_path {
            ConnectionManager::new_file(path)?
        } else {
            ConnectionManager::new_memory()?
        };

        // Mounted by the ordinary handle opener like every other mode;
        // `None` when the in-memory engine refused, which degrades
        // `repl::*` loudly and never blocks access to the user's database.
        let (mut handle, repl_namespace_available) =
            crate::connection::open_handle_over(repl_db.clone())?;

        // mount! the user database as "main" if specified
        if let Some(ref path) = db_path {
            let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
            crate::exec_ng::run_dql_query(
                &format!("mount!(\"{}\", \"main\")(*)", path),
                &mut *session,
            )?;
        }

        let dql_handle = Arc::new(std::sync::Mutex::new(handle));

        let shared_info = SharedReplState::new(std::env::args().collect(), db_path.clone());

        // ONE configuration authority: the controller takes this config's
        // budgets and its SHARED helper policy, then the same config moves
        // into the state — startup cannot mint two authorities.
        let config = super::config::ReplConfig::new(output_format);
        let parser_worker = Arc::new(super::parser_worker::ParserWorkerController::new(
            *config.parser_budgets(),
            Arc::clone(config.editor_helper_policy()),
            repl_db.clone(),
            None,
        ));

        let mut state = Self {
            db_path,
            last_query: None,
            last_execution_time: None,
            shared_info,
            db_connection,
            dql_handle,
            config,
            name_generator: super::name_generator::ReplNameGenerator::new(),
            captures: Vec::new(),
            repl_namespace_initialized: false,
            parser_worker,
            repl_db,
            repl_namespace_available,
        };
        state.seed_config_options();
        state.sync_shared_config();
        Ok(state)
    }

    /// The typed configuration, read-only. Mutation crosses the operations
    /// below.
    pub fn config(&self) -> &super::config::ReplConfig {
        &self.config
    }

    /// Seed every `repl::config.option` row from the typed state.
    fn seed_config_options(&self) {
        let Some(db) = &self.repl_db else { return };
        for (name, value, kind, default) in self.config.option_rows() {
            if let crate::client::database::WriteOutcome::Lost(reason) = db.set_option(
                name,
                Some(value),
                kind,
                Some(default.to_string()),
                "startup",
            ) {
                crate::client::incident::warning(
                    "ledger",
                    crate::client::incident::hierarchy::LEDGER_WRITE_LOST,
                    format!("repl::config.option '{name}' seed failed ({reason})"),
                );
            }
        }
    }

    /// Re-project ONE option row from the typed state. A lost write is loud;
    /// terminal behavior still follows the typed value, and a queued write
    /// retries before the next prompt — the row never silently claims an old
    /// value without a warning.
    fn project_config_option(&self, name: &'static str, source: &str) {
        let Some(db) = &self.repl_db else { return };
        let Some((_, value, kind, default)) = self
            .config
            .option_rows()
            .into_iter()
            .find(|(n, ..)| *n == name)
        else {
            return;
        };
        if let crate::client::database::WriteOutcome::Lost(reason) =
            db.set_option(name, Some(value), kind, Some(default.to_string()), source)
        {
            crate::client::incident::warning(
                "ledger",
                crate::client::incident::hierarchy::LEDGER_WRITE_LOST,
                format!(
                    "repl::config.option '{name}' projection failed ({reason}); \
                     the typed value stands"
                ),
            );
        }
    }

    // --- typed configuration operations: validate, change the typed value,
    // --- project the option row, refresh the TUI snapshot — one act.

    pub fn set_output_format(&mut self, format: OutputFormat, source: &str) {
        self.config.set_output_format(format);
        self.project_config_option("output_format", source);
        self.sync_shared_config();
    }

    pub fn set_target_stage(&mut self, stage: Option<Stage>, source: &str) {
        self.config.set_target_stage(stage);
        self.project_config_option("target_stage", source);
        self.sync_shared_config();
    }

    pub fn set_input_mode_sql(&mut self, sql: bool, source: &str) {
        self.config.set_input_mode_sql(sql);
        self.project_config_option("input_mode", source);
        self.sync_shared_config();
    }

    pub fn set_zebra_mode(&mut self, colors: usize, source: &str) -> Result<(), String> {
        self.config.set_zebra_mode(colors)?;
        self.project_config_option("zebra_columns", source);
        self.sync_shared_config();
        Ok(())
    }

    pub fn set_no_headers(&mut self, no_headers: bool, source: &str) {
        self.config.set_no_headers(no_headers);
        self.project_config_option("headers", source);
        self.sync_shared_config();
    }

    pub fn set_show_meta_output(&mut self, show: bool, source: &str) {
        self.config.set_show_meta_output(show);
        self.project_config_option("meta_output", source);
        self.sync_shared_config();
    }

    pub fn set_multiline(&mut self, multiline: bool, source: &str) {
        self.config.set_multiline(multiline);
        self.project_config_option("multiline", source);
        self.sync_shared_config();
    }

    /// Manual breaker control: change the shared policy, project the row,
    /// refresh the snapshot — one act. Enabling arms the breaker again.
    pub fn set_editor_parser_helpers(&mut self, enabled: bool, source: &str) {
        self.config.set_editor_parser_helpers(enabled);
        self.project_config_option("editor_parser_helpers", source);
        self.sync_shared_config();
    }

    /// Refresh the WHOLE TUI snapshot before a launch: the config fields
    /// and the Window C history ring, both projected from their authorities
    /// (the typed config, the input ledger). The snapshot is a presentation
    /// cache; nothing reads it back.
    pub fn prepare_tui_snapshot(&mut self) {
        self.sync_shared_config();
        if let Some(db) = &self.repl_db {
            if let Ok(rows) = db.history_rows() {
                let entries: Vec<super::info_panel::QueryHistoryEntry> = rows
                    .into_iter()
                    .filter(|row| row.kind == "dql" && row.outcome == "succeeded")
                    .filter_map(|row| {
                        row.generated_sql
                            .map(|sql| super::info_panel::QueryHistoryEntry {
                                dql: row.input,
                                sql,
                            })
                    })
                    .collect();
                let start = entries.len().saturating_sub(50);
                self.shared_info.query_history = entries[start..].to_vec();
            }
        }
    }

    /// Sync current config into shared_info for TUI display. The snapshot
    /// is a presentation cache written FROM the typed state, never an
    /// authority.
    pub fn sync_shared_config(&mut self) {
        let output_format = self.config.output_format_rendered();
        let target_stage = self.config.target_stage_rendered().to_string();
        self.shared_info.sync_config(
            &output_format,
            &target_stage,
            self.config.sql_mode(),
            self.config.zebra_mode(),
            self.config.no_headers(),
            self.config.multiline(),
            self.config.editor_helpers_enabled(),
        );
    }
}

pub enum CommandResult {
    Continue,
    Exit,
}

/// What the prompt's recovery boundary decided.
///
/// A REPL prompt is a recovery boundary: an ordinary DQL prompt is never
/// presented backed by a quarantined session. The decision is taken over the
/// TYPED health report — never by matching error text.
pub enum ReplRecovery {
    /// The session is healthy; present the prompt.
    NotNeeded,
    /// The session was quarantined and has been replaced. The strings are
    /// the report the host prints: the incident, and what the reset rebuilt,
    /// lost, and retained.
    Recovered {
        incident: String,
        rebuilt: String,
        lost: String,
        retained: String,
    },
    /// Recovery failed. The session remains quarantined; the host must
    /// report a terminal connection failure instead of presenting a prompt.
    Terminal { incident: String, failure: String },
}

/// The recovery decision, over the typed API alone. Re-mounting the host's
/// database after a successful reset is part of "replace the session": a
/// prompt over an unmounted main is not the session the user was in, and a
/// failed re-mount is a failed recovery.
pub fn recover_repl_session(
    handle: &mut dyn delightql_core::api::DqlHandle,
    db_path: Option<&str>,
) -> ReplRecovery {
    use delightql_core::api::SessionHealthReport;
    let (operation, message) = match handle.session_health() {
        SessionHealthReport::Healthy => return ReplRecovery::NotNeeded,
        SessionHealthReport::Quarantined { operation, message } => (operation, message),
    };
    let incident = format!("{operation}: {message}");
    let recovery = match handle.recover_session() {
        Ok(recovery) => recovery,
        Err(failure) => return ReplRecovery::Terminal { incident, failure },
    };
    let mut lost = recovery.lost;
    if let Some(path) = db_path {
        let remount = handle
            .session()
            .map_err(|e| e.to_string())
            .and_then(|mut session| {
                crate::exec_ng::run_dql_query(
                    &format!("mount!(\"{}\", \"main\")(*)", path),
                    &mut *session,
                )
                .map_err(|e| e.to_string())
            });
        match remount {
            Ok(_) => lost.push_str(&format!("; '{path}' was re-mounted as main")),
            Err(failure) => {
                return ReplRecovery::Terminal {
                    incident,
                    failure: format!(
                        "the session was reset but '{path}' failed to re-mount: {failure}"
                    ),
                }
            }
        }
    }
    ReplRecovery::Recovered {
        incident,
        rebuilt: recovery.rebuilt,
        lost,
        retained: recovery.retained,
    }
}

/// Run the recovery boundary before a prompt is presented: report, recover
/// or terminate, and reset the REPL bookkeeping that lived in the replaced
/// session. Returns `Exit` exactly when recovery failed.
pub fn prompt_recovery_boundary(repl_state: &mut ReplState) -> CommandResult {
    let decision = {
        let mut handle = repl_state
            .dql_handle
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        recover_repl_session(&mut **handle, repl_state.db_path.as_deref())
    };
    match decision {
        ReplRecovery::NotNeeded => {
            // The prompt boundary is also the pending-write boundary: a
            // write queued behind a busy connection flushes before the next
            // prompt, and a loss is loud — never a silent disappearance
            // from repl::*, dumps, and bug reports.
            if let Some(db) = &repl_state.repl_db {
                db.drain_panics();
                let (_, lost, reason) = db.flush_pending();
                if lost > 0 {
                    crate::client::incident::warning(
                        "ledger",
                        crate::client::incident::hierarchy::LEDGER_WRITE_LOST,
                        format!(
                            "{lost} pending repl-database writes were lost ({})",
                            reason.unwrap_or_default()
                        ),
                    );
                }
            }
            CommandResult::Continue
        }
        ReplRecovery::Recovered {
            incident,
            rebuilt,
            lost,
            retained,
        } => {
            eprintln!("!! the session was quarantined after {incident}");
            eprintln!("!! the session was reset before this prompt:");
            eprintln!("!!   rebuilt:  {rebuilt}");
            eprintln!("!!   lost:     {lost}");
            eprintln!("!!   retained: {retained}");
            // The repl:: capture namespace and its views lived in the
            // replaced session's catalog.
            repl_state.repl_namespace_initialized = false;
            repl_state.captures.clear();
            // The live client database survived the reset; the catalog
            // mapping did not. Remount, reinstall the projections, verify,
            // and flush pending client writes — all before another prompt.
            // Failure is degraded client diagnostics, never disguised as a
            // successful restoration and never a reason to block the user's
            // database.
            repl_state.repl_namespace_available = false;
            if let Some(db) = repl_state.repl_db.clone() {
                let reinstall = {
                    let mut handle = repl_state
                        .dql_handle
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    crate::client::mount::install_repl_namespace(&mut **handle)
                };
                match reinstall {
                    Ok(()) => repl_state.repl_namespace_available = true,
                    Err(e) => {
                        eprintln!("!! degraded: the repl::* namespace could not be restored ({e})")
                    }
                }
                let (_, lost_writes, reason) = db.flush_pending();
                if lost_writes > 0 {
                    eprintln!(
                        "!! degraded: {lost_writes} pending repl-database writes were lost ({})",
                        reason.unwrap_or_default()
                    );
                }
            }
            CommandResult::Continue
        }
        ReplRecovery::Terminal { incident, failure } => {
            eprintln!("!! the session was quarantined after {incident}");
            eprintln!("!! session recovery failed: {failure}");
            eprintln!("!! terminal connection failure — the session remains quarantined; closing this REPL");
            CommandResult::Exit
        }
    }
}

/// Check if input is a dot command
pub fn is_dot_command(input: &str) -> bool {
    input.trim().starts_with('.')
}

/// One REPL dot command, as data.
pub struct DotCommand {
    pub name: &'static str,
    pub aliases: &'static [&'static str],
    /// Argument hint for display, "" when the command takes none.
    pub args: &'static str,
    /// Help-screen section this command renders under.
    pub section: &'static str,
    pub summary: &'static str,
    /// Worked example for the help screen, "" when self-evident.
    pub example: &'static str,
}

/// THE enumerable dot-command surface — every other surface is a
/// projection of this table: the `.help` screen, tab completion, and
/// `cli::surface`'s `dot_command` rows all render from it, and the
/// dispatcher's match arms are welded to it in both directions by
/// `registry_and_dispatch_agree`. Adding an arm without a row (or a row
/// without an arm) fails that test — there is no second source to drift.
pub const DOT_COMMANDS: &[DotCommand] = &[
    DotCommand {
        name: ".help",
        aliases: &[],
        args: "",
        section: "General",
        summary: "Show this help message",
        example: "",
    },
    DotCommand {
        name: ".exit",
        aliases: &[".quit"],
        args: "",
        section: "General",
        summary: "Exit the REPL",
        example: "",
    },
    DotCommand {
        name: ".info",
        aliases: &[],
        args: "",
        section: "Display & Output",
        summary: "Show the multi-pane TUI (Ctrl-X, t toggles while typing; a lone `.` too)",
        example: "",
    },
    DotCommand {
        name: ".format",
        aliases: &[],
        args: "[FORMAT]",
        section: "Display & Output",
        summary: "Set or show output format (table, json, csv, tsv, list)",
        example: "",
    },
    DotCommand {
        name: ".zebra",
        aliases: &[],
        args: "[0-4]",
        section: "Display & Output",
        summary: "Column coloring (0=off [default], 2=blue/cyan, 3=RWB, 4=RWBG)",
        example: "",
    },
    DotCommand {
        name: ".to",
        aliases: &[],
        args: "[STAGE]",
        section: "Display & Output",
        summary: "Show output stage (cst, ast-unresolved, ast-resolved, etc.)",
        example: "",
    },
    DotCommand {
        name: ".dql",
        aliases: &[],
        args: "[query]",
        section: "Mode Commands",
        summary: "Switch to DQL mode (default); with a query, execute one-off",
        example: "",
    },
    DotCommand {
        name: ".sql",
        aliases: &[],
        args: "[query]",
        section: "Mode Commands",
        summary: "Switch to SQL mode; with a query, execute one-off",
        example: "",
    },
    DotCommand {
        name: ".multiline",
        aliases: &[],
        args: "[on|off]",
        section: "Mode Commands",
        summary: "Toggle multiline input mode (default: on)",
        example: "",
    },
    DotCommand {
        name: ".bug",
        aliases: &[],
        args: "[description…]",
        section: "File & Diagnostics",
        summary: "Write this session's error.log, context and replay-script, plus the databases and DDL it used, as bug-<stamp>.tgz",
        example: ".bug the join drops rows after the second pipe",
    },
    DotCommand {
        name: ".repl",
        aliases: &[],
        args: "helpers [status|on|off]",
        section: "File & Diagnostics",
        summary: "Inspect or control the optional parser helpers (coloring, parse-aware prompt, continuation navigation); submission preflight stays on",
        example: ".repl helpers status",
    },
];

/// Every spelling the dispatcher accepts (names + aliases), in registry
/// order — the projection consumed by tab completion and the surface rows.
/// The registry projected into `repl::surface.dot_command` rows: one per
/// accepted spelling, aliases pointing at the canonical spelling.
pub fn dot_command_surface() -> Vec<crate::client::database::SurfaceRow> {
    use crate::client::database::SurfaceRow;
    DOT_COMMANDS
        .iter()
        .flat_map(|cmd| {
            let row = |spelling: &'static str, is_alias: bool| SurfaceRow {
                spelling,
                canonical_name: cmd.name,
                is_alias,
                args: cmd.args,
                section: cmd.section,
                summary: cmd.summary,
                example: cmd.example,
            };
            std::iter::once(row(cmd.name, false))
                .chain(cmd.aliases.iter().map(move |alias| row(alias, true)))
        })
        .collect()
}

pub fn dot_command_spellings() -> impl Iterator<Item = &'static str> {
    DOT_COMMANDS
        .iter()
        .flat_map(|c| std::iter::once(c.name).chain(c.aliases.iter().copied()))
}

/// Handle a dot command, recorded in the one ordered input ledger: the row
/// opens as `started` before dispatch, and closes with the dispatched
/// outcome — an unknown spelling is `refused`, an error is `failed`, and
/// `.exit` closes successfully before the client exits.
pub fn handle_dot_command(cmd: &str, repl_state: &mut ReplState) -> Result<CommandResult> {
    let cmd = cmd.trim();
    let parts: Vec<&str> = cmd.split_whitespace().collect();

    if parts.is_empty() {
        return Ok(CommandResult::Continue);
    }

    use crate::client::database::{InputKind, InputOutcome};
    let ledger_id = repl_state.repl_db.as_ref().map(|db| {
        let (id, outcome) = db.record_input(InputKind::DotCommand, cmd);
        note_lost_ledger_write(&outcome);
        id
    });
    let known = dot_command_spellings().any(|spelling| spelling == parts[0]);
    let started = Instant::now();

    let result = dispatch_dot_command(cmd, &parts, repl_state);

    if let (Some(id), Some(db)) = (ledger_id, repl_state.repl_db.clone()) {
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        let (outcome, error) = match (&result, known) {
            (Err(e), _) => (InputOutcome::Failed, Some(e.to_string())),
            (Ok(_), false) => (InputOutcome::Refused, None),
            (Ok(_), true) => (InputOutcome::Succeeded, None),
        };
        note_lost_ledger_write(&db.close_input(id, outcome, error, None, Some(elapsed_ms)));
    }
    result
}

/// History write failures are loud and bounded, and never change dispatch:
/// a lost row costs one warning line, nothing else.
fn note_lost_ledger_write(outcome: &crate::client::database::WriteOutcome) {
    if let crate::client::database::WriteOutcome::Lost(reason) = outcome {
        crate::client::incident::warning(
            "ledger",
            crate::client::incident::hierarchy::LEDGER_WRITE_LOST,
            format!("a repl::history.input write was lost ({reason})"),
        );
    }
}

/// The dispatcher proper — the arms the registry is welded to.
fn dispatch_dot_command(
    cmd: &str,
    parts: &[&str],
    repl_state: &mut ReplState,
) -> Result<CommandResult> {
    match parts[0] {
        ".exit" | ".quit" => Ok(CommandResult::Exit),

        ".info" => {
            // Switch to multi-pane TUI
            repl_state.prepare_tui_snapshot();
            let handle = repl_state.dql_handle.clone();
            let connection = repl_state.db_connection.clone();
            let final_window_position = super::multi_pane_tui::run_multi_pane_tui(
                repl_state.shared_info.clone(),
                handle,
                connection,
            )?;
            // Update the persistent position
            repl_state.shared_info.last_window_position = Some(final_window_position);
            println!(); // Clean line after returning
            Ok(CommandResult::Continue)
        }

        ".help" => {
            print_help();
            Ok(CommandResult::Continue)
        }

        ".format" => {
            if parts.len() > 1 {
                match OutputFormat::from_str(parts[1]) {
                    Some(format) => {
                        repl_state.set_output_format(format, ".format");
                        if repl_state.config().show_meta_output() {
                            println!("Output format set to: {:?}", format);
                        }
                    }
                    None => {
                        eprintln!(
                            "Invalid format '{}'. Available formats: {}",
                            parts[1],
                            OutputFormat::all_formats().join(", ")
                        );
                    }
                }
            } else if repl_state.config().show_meta_output() {
                println!(
                    "Current output format: {:?}",
                    repl_state.config().output_format()
                );
                println!(
                    "Available formats: {}",
                    OutputFormat::all_formats().join(", ")
                );
            }
            Ok(CommandResult::Continue)
        }

        ".sql" => {
            if parts.len() > 1 {
                // Execute one-off SQL query while staying in current mode
                let sql_query = cmd[4..].trim(); // Skip ".sql" prefix
                if repl_state.config().show_meta_output() {
                    println!("Executing SQL: {}", sql_query);
                }
                // One-off in a NAMED mode, through the ledger-owning road —
                // the submission gets its own `sql` row beside this
                // dot-command row, exactly as `.dql` one-offs do.
                let dummy_flag = std::sync::atomic::AtomicBool::new(false);
                process_query_in_mode(sql_query, repl_state, &dummy_flag, true)?;
            } else {
                // Set SQL mode explicitly
                repl_state.set_input_mode_sql(true, ".sql");
                if repl_state.config().show_meta_output() {
                    println!("SQL mode enabled - queries will be executed as raw SQL");
                }
            }
            Ok(CommandResult::Continue)
        }

        ".dql" => {
            if parts.len() > 1 {
                // Execute one-off DQL query while staying in current mode
                let dql_query = cmd[4..].trim(); // Skip ".dql" prefix
                if repl_state.config().show_meta_output() {
                    println!("Executing DQL: {}", dql_query);
                }
                // One-off: the mode is named for this execution, not flipped
                // in the configuration.
                let dummy_flag = std::sync::atomic::AtomicBool::new(false);
                process_query_in_mode(dql_query, repl_state, &dummy_flag, false)?;
            } else {
                // Set DQL mode explicitly
                repl_state.set_input_mode_sql(false, ".dql");
                if repl_state.config().show_meta_output() {
                    println!("DQL mode enabled - queries will be parsed as DelightQL");
                }
            }
            Ok(CommandResult::Continue)
        }

        ".zebra" => {
            if parts.len() > 1 {
                match parts[1].parse::<usize>() {
                    Ok(n) if n <= 4 => {
                        // The typed operation validates; 0/1 disables.
                        let _ = repl_state.set_zebra_mode(n, ".zebra");
                        if repl_state.config().show_meta_output() {
                            match repl_state.config().zebra_mode() {
                                None => println!("Zebra mode disabled"),
                                Some(n) => {
                                    let color_desc = match n {
                                        2 => "blue and cyan",
                                        3 => "red, white, and blue",
                                        4 => "red, white, blue, and green",
                                        _ => unreachable!(),
                                    };
                                    println!(
                                        "Zebra mode enabled with {} colors: {}",
                                        n, color_desc
                                    );
                                }
                            }
                        }
                    }
                    Ok(_) => {
                        eprintln!("Zebra mode supports 2-4 colors only");
                        eprintln!("Use .zebra 0 to disable");
                    }
                    Err(_) => {
                        eprintln!("Invalid number. Usage: .zebra <2-4>");
                        eprintln!("  .zebra 2  - blue and cyan");
                        eprintln!("  .zebra 3  - red, white, and blue");
                        eprintln!("  .zebra 4  - red, white, blue, and green");
                        eprintln!("  .zebra 0  - disable zebra mode");
                    }
                }
            } else {
                // Show current zebra mode
                if repl_state.config().show_meta_output() {
                    match repl_state.config().zebra_mode() {
                        None => println!("Zebra mode is disabled"),
                        Some(n) => {
                            let color_desc = match n {
                                2 => "blue and cyan",
                                3 => "red, white, and blue",
                                4 => "red, white, blue, and green",
                                _ => "unknown",
                            };
                            println!("Zebra mode is enabled with {} colors: {}", n, color_desc);
                        }
                    }
                }
            }
            Ok(CommandResult::Continue)
        }

        ".to" => {
            if parts.len() > 1 {
                // Parse the stage
                let stage_str = parts[1];
                match stage_str {
                    "cst" => {
                        repl_state.set_target_stage(Some(Stage::Cst), ".to");
                        if repl_state.config().show_meta_output() {
                            println!("Output stage set to: CST");
                        }
                    }
                    "ast-unresolved" => {
                        repl_state.set_target_stage(Some(Stage::AstUnresolved), ".to");
                        if repl_state.config().show_meta_output() {
                            println!("Output stage set to: Unresolved AST");
                        }
                    }
                    "ast-resolved" => {
                        repl_state.set_target_stage(Some(Stage::AstResolved), ".to");
                        if repl_state.config().show_meta_output() {
                            println!("Output stage set to: Resolved AST");
                        }
                    }
                    "ast-refined" => {
                        repl_state.set_target_stage(Some(Stage::AstRefined), ".to");
                        if repl_state.config().show_meta_output() {
                            println!("Output stage set to: Refined AST");
                        }
                    }
                    "ast-sql" | "sql-ast" => {
                        repl_state.set_target_stage(Some(Stage::AstSql), ".to");
                        if repl_state.config().show_meta_output() {
                            println!("Output stage set to: SQL AST");
                        }
                    }
                    "sql" => {
                        repl_state.set_target_stage(Some(Stage::Sql), ".to");
                        if repl_state.config().show_meta_output() {
                            println!("Output stage set to: SQL");
                        }
                    }
                    "results" => {
                        repl_state.set_target_stage(None, ".to");
                        if repl_state.config().show_meta_output() {
                            println!("Output stage set to: Results (default)");
                        }
                    }
                    "hash" => {
                        repl_state.set_target_stage(Some(Stage::Hash), ".to");
                        if repl_state.config().show_meta_output() {
                            println!("Output stage set to: Hash");
                        }
                    }
                    "fingerprint" => {
                        repl_state.set_target_stage(Some(Stage::Fingerprint), ".to");
                        if repl_state.config().show_meta_output() {
                            println!("Output stage set to: Fingerprint");
                        }
                    }
                    _ => {
                        eprintln!("Invalid stage '{}'. Available stages:", stage_str);
                        eprintln!("  cst, ast-unresolved, ast-resolved, ast-refined, sql-ast, sql, results, hash, fingerprint");
                    }
                }
            } else {
                // Show current stage
                if repl_state.config().show_meta_output() {
                    match repl_state.config().target_stage() {
                        None => println!("Current output stage: Results (default)"),
                        Some(Stage::Cst) => println!("Current output stage: CST"),
                        Some(Stage::AstUnresolved) => {
                            println!("Current output stage: Unresolved AST")
                        }
                        Some(Stage::AstResolved) => println!("Current output stage: Resolved AST"),
                        Some(Stage::AstRefined) => println!("Current output stage: Refined AST"),
                        Some(Stage::AstSql) => println!("Current output stage: SQL AST"),
                        Some(Stage::Sql) => println!("Current output stage: SQL"),
                        Some(Stage::Results) => println!("Current output stage: Results"),
                        Some(Stage::Fingerprint) => println!("Current output stage: Fingerprint"),
                        Some(Stage::Hash) => println!("Current output stage: Hash"),
                        Some(Stage::ByteHash) => println!("Current output stage: ByteHash"),
                        Some(Stage::TotalHash) => println!("Current output stage: TotalHash"),
                    }
                    println!("Available stages: cst, ast-unresolved, ast-resolved, ast-refined, sql-ast, sql, results, hash, bhash, totalhash, fingerprint");
                }
            }
            Ok(CommandResult::Continue)
        }

        ".bug" => {
            // The description is the rest of the line, verbatim.
            let description = cmd.trim_start().strip_prefix(".bug").unwrap_or("").trim();
            handle_bug_command(repl_state, description)?;
            Ok(CommandResult::Continue)
        }

        ".multiline" => {
            if parts.len() > 1 {
                match parts[1] {
                    "on" => repl_state.set_multiline(true, ".multiline"),
                    "off" => repl_state.set_multiline(false, ".multiline"),
                    _ => eprintln!("Usage: .multiline [on|off]"),
                }
            } else {
                let toggled = !repl_state.config().multiline();
                repl_state.set_multiline(toggled, ".multiline");
            }
            if repl_state.config().show_meta_output() {
                println!(
                    "Multiline mode: {}",
                    if repl_state.config().multiline() {
                        "on"
                    } else {
                        "off"
                    }
                );
            }
            Ok(CommandResult::Continue)
        }

        ".repl" => {
            handle_repl_command(parts, repl_state);
            Ok(CommandResult::Continue)
        }

        _ => {
            eprintln!("Unknown command: {}", parts[0]);
            eprintln!("Type '.help' for available commands");
            Ok(CommandResult::Continue)
        }
    }
}

/// `.repl dump <path>` — the one road by which the live client database
/// reaches disk outside a bug report. The dump is a snapshot: it can contain
/// sensitive literals and paths (authored inputs, exact timed-out text —
/// deliberately unredacted, because a transformed input is not a faithful
/// reproducer), and the live database continues collecting afterwards. A
/// failed dump leaves the live database untouched.
fn handle_repl_command(parts: &[&str], repl_state: &mut ReplState) {
    match parts {
        // The omitted action reads as `status`.
        [_, "helpers"] | [_, "helpers", "status"] => {
            let state = if repl_state.config().editor_helpers_enabled() {
                "on"
            } else {
                "off"
            };
            println!(
                "Optional parser helpers (prompt well-formedness, syntax coloring, \
                 continuation navigation): {state}"
            );
            println!("Submission safety preflight: always on");
        }
        [_, "helpers", "on"] => {
            repl_state.set_editor_parser_helpers(true, ".repl helpers");
            if repl_state.config().show_meta_output() {
                println!("Optional parser helpers re-enabled; the breaker is armed again.");
            }
        }
        [_, "helpers", "off"] => {
            repl_state.set_editor_parser_helpers(false, ".repl helpers");
            if repl_state.config().show_meta_output() {
                println!(
                    "Optional parser helpers disabled. Submission safety preflight \
                     remains enabled."
                );
            }
        }
        _ => {
            eprintln!("Usage: .repl helpers [status|on|off]");
            eprintln!(
                "dump writes a snapshot of the live REPL database (inputs, configuration,                  timeout evidence). The snapshot can contain sensitive literals and                  paths from this session."
            );
            eprintln!(
                "helpers inspects or sets the optional parser assistance (prompt                  well-formedness, syntax coloring, continuation navigation).                  Submission safety preflight is not configurable."
            );
        }
    }
}


/// `.bug [description…]`: the session files now, the client database
/// serialized, and every database and DDL file the session mounted, as
/// one tarball beside the session files. The words become an info row
/// so they travel in error.log with the incidents they describe.
fn handle_bug_command(repl_state: &mut ReplState, description: &str) -> Result<()> {
    let Some(db) = repl_state.repl_db.clone() else {
        crate::client::incident::error(
            "dot_command",
            crate::client::incident::hierarchy::DATABASE_UNAVAILABLE,
            "no client database this session; there is nothing to report from".to_string(),
        );
        return Ok(());
    };
    let primary = repl_state.db_path.clone().map(std::path::PathBuf::from);
    let report = {
        let mut handle = repl_state
            .dql_handle
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        crate::client::bug::write_bug_report(
            &db,
            &mut **handle,
            (!description.is_empty()).then_some(description),
            primary.as_deref(),
        )
    };
    match report {
        Ok(report) => {
            println!("bug report: {}", report.archive.display());
            println!(
                "  {} database file(s), {} DDL file(s), repl.sqlite, and the session files",
                report.databases.len(),
                report.ddl_files.len()
            );
            println!("  replay with: dql query --replay-repl {}", report.archive.display());
        }
        Err(e) => crate::client::incident::error(
            "dot_command",
            crate::client::incident::hierarchy::CONFIG,
            format!("the bug report could not be written: {e}"),
        ),
    }
    Ok(())
}

/// Print help message
fn print_help() {
    println!("DelightQL REPL Commands:");
    // Dot commands render FROM the registry — this screen cannot know a
    // command the dispatcher doesn't, or miss one it does.
    let mut current_section = "";
    for cmd in DOT_COMMANDS {
        if cmd.section != current_section {
            println!();
            println!("{}:", cmd.section);
            current_section = cmd.section;
        }
        let mut invocation = cmd.name.to_string();
        for alias in cmd.aliases {
            invocation.push_str(", ");
            invocation.push_str(alias);
        }
        if !cmd.args.is_empty() {
            invocation.push(' ');
            invocation.push_str(cmd.args);
        }
        if invocation.len() <= 18 {
            println!("  {:<18} {}", invocation, cmd.summary);
        } else {
            println!("  {}", invocation);
            println!("  {:<18} {}", "", cmd.summary);
        }
        if !cmd.example.is_empty() {
            println!("  {:<18} Example: {}", "", cmd.example);
        }
    }
    println!();
    println!("Keyboard Shortcuts:");
    println!("  Enter              Continue query (multiline on) or execute (multiline off)");
    println!("  Enter (empty line) Submit accumulated query (multiline on)");
    println!("  Alt+Enter          Insert newline within current line");
    println!("  Ctrl+C             Cancel partial input (multiline on)");
    println!("  Ctrl-X, t          Toggle multi-pane TUI (H/J/K/L to navigate)");
    println!("  Ctrl-B / Ctrl-F    Jump to the previous / next continuation");
    println!("  Ctrl-X, d / D      Delete to the next / previous continuation");
    println!();
    println!("Query Examples:");
    println!("  users(*) |> (name, email)");
    println!("  products(*), price > 10 ~> avg:(price)");
    println!();
    println!("Introspection (Meta-Circular System):");
    println!("  sys::cartridges.cartridge(*)       List all installed cartridges");
    println!("  sys::entities.entity(*)            List all discovered entities");
    println!("  sys::ns.namespace(*)               List all namespaces");
    println!("  sys::ns.activated_entity(*)        List entity activations");
}

/// Process a query using the new pipeline, in the configured input mode.
pub fn process_query(
    query: &str,
    repl_state: &mut ReplState,
    interrupted_flag: &std::sync::atomic::AtomicBool,
) -> Result<()> {
    let sql_mode = repl_state.config().sql_mode();
    process_query_in_mode(query, repl_state, interrupted_flag, sql_mode)
}

/// The same, in a NAMED mode — the `.dql`/`.sql` one-offs execute in their
/// own mode without flipping the configuration.
pub fn process_query_in_mode(
    query: &str,
    repl_state: &mut ReplState,
    interrupted_flag: &std::sync::atomic::AtomicBool,
    sql_mode: bool,
) -> Result<()> {
    use std::sync::{atomic::Ordering, mpsc};
    use std::thread;
    use std::time::Duration;

    let start_time = Instant::now();

    repl_state.last_query = Some(query.to_string());

    // The one ordered ledger: the submission opens as `started` and closes
    // with its dispatched outcome below.
    let ledger_kind = if sql_mode {
        crate::client::database::InputKind::Sql
    } else {
        crate::client::database::InputKind::Dql
    };
    let ledger_id = repl_state.repl_db.as_ref().map(|db| {
        let (id, outcome) = db.record_input(ledger_kind, query);
        note_lost_ledger_write(&outcome);
        id
    });
    let close_ledger = |repl_state: &ReplState,
                        outcome: crate::client::database::InputOutcome,
                        error: Option<String>,
                        sql: Option<String>,
                        elapsed_ms: Option<f64>| {
        if let (Some(id), Some(db)) = (ledger_id, repl_state.repl_db.as_ref()) {
            note_lost_ledger_write(&db.close_input(id, outcome, error, sql, elapsed_ms));
        }
    };

    // Update shared info for multi-pane TUI
    repl_state.shared_info.update(query, None);

    // Submission preflight: the EXACT submitted bytes, at the exact entrance
    // the compiler will take, parsed inside the containment worker BEFORE
    // the in-process compiler sees them. The gate FAILS CLOSED: only a
    // validated Preflight answer admits the bytes to the in-process
    // compiler — whatever the verdict, the compiler is the judge of
    // defects. A timeout refuses with its incident recorded; a worker that
    // cannot serve (after one bounded retry against the already-spawned
    // replacement) refuses DISTINCTLY, because the in-process parser is
    // unkillable and crossing it without a containment verdict is exactly
    // the freeze this boundary exists to prevent.
    if !sql_mode {
        use super::parser_worker::ProbeOutcome;
        use super::worker::WorkerResult;
        let mut refusal: Option<String> = None;
        for attempt in 0..2 {
            match repl_state.parser_worker.probe(
                super::config::ReplParserOperation::SubmissionPreflight,
                query,
                None,
            ) {
                ProbeOutcome::Answer(WorkerResult::Preflight { .. }) => {
                    refusal = None;
                    break;
                }
                ProbeOutcome::TimedOut => {
                    refusal = Some("submission preflight exceeded its parser budget".to_string());
                    break;
                }
                // The worker panicked on these exact bytes and said so; the
                // in-process parser reads the same bytes through the same
                // code, so crossing it would be the freeze this boundary
                // exists to prevent — with a panic in place of a freeze.
                ProbeOutcome::Panicked { message, recorded } => {
                    let record = match recorded {
                        crate::client::database::IncidentRecordOutcome::Recorded {
                            incident_id,
                        } => format!("repl::errors.incident #{incident_id}"),
                        crate::client::database::IncidentRecordOutcome::Queued { .. } => {
                            "repl::errors.incident (pending)".to_string()
                        }
                        crate::client::database::IncidentRecordOutcome::Lost(reason) => {
                            format!("NOT recorded: {reason}")
                        }
                    };
                    refusal = Some(format!(
                        "the parser worker panicked on this submission ({message}); \
                         see {record}"
                    ));
                    break;
                }
                // probe() never answers Disabled for the mandatory
                // preflight — the breaker is not consulted for it. If it
                // ever did, the gate still fails CLOSED.
                ProbeOutcome::Disabled => {
                    refusal = Some("the parser containment worker is unavailable".to_string());
                    break;
                }
                // A probe that could not serve replaced its worker; one
                // retry meets the replacement, then the gate closes.
                ProbeOutcome::Unavailable | ProbeOutcome::Answer(_) if attempt == 0 => {
                    refusal = Some("the parser containment worker is unavailable".to_string());
                }
                ProbeOutcome::Unavailable | ProbeOutcome::Answer(_) => {
                    refusal = Some("the parser containment worker is unavailable".to_string());
                    break;
                }
            }
        }
        if let Some(reason) = refusal {
            // The refusal is its own incident: the submission never
            // reached the compiler, so nothing else records that it was
            // turned away, or why.
            let said = if reason.contains("budget") {
                "the parser preflight for this submission exceeded its budget; \
                 the submission is refused and its exact input is recorded in \
                 repl::errors.incident"
                    .to_string()
            } else if reason.contains("panicked") {
                format!("{reason}; the submission is refused")
            } else {
                "the parser containment worker is unavailable, so this \
                 submission cannot be preflighted; it is refused rather than \
                 handed to the in-process parser without a verdict"
                    .to_string()
            };
            eprintln!("error: {said}");
            if let Some(db) = &repl_state.repl_db {
                use crate::client::incident::{hierarchy, Incident, IncidentKind};
                let mut incident = Incident::plain(
                    IncidentKind::Error,
                    "preflight",
                    hierarchy::PREFLIGHT_REFUSED,
                    said,
                );
                incident.input = Some(query.to_string());
                db.record_incident(incident);
            }
            close_ledger(
                repl_state,
                crate::client::database::InputOutcome::Refused,
                Some(reason),
                None,
                Some(start_time.elapsed().as_secs_f64() * 1000.0),
            );
            return Ok(());
        }
    }

    // Reset the interrupted flag before starting
    interrupted_flag.store(false, Ordering::Relaxed);

    // Clone what we need for the thread
    let query_str = query.to_string();
    let target_stage = repl_state.config().target_stage();
    let output_format = repl_state.config().output_format();
    let db_connection = repl_state.db_connection.clone(); // Clone the connection
    let zebra_mode = repl_state.config().zebra_mode();
    let no_headers = repl_state.config().no_headers();
    let dql_handle = Arc::clone(&repl_state.dql_handle); // Clone Arc reference for thread

    // Get interrupt handle BEFORE spawning thread (SQLite only)
    // This ensures we can interrupt the actual connection being used
    let interrupt_handle = match &repl_state.db_connection {
        ConnectionManager::SQLite(_) => {
            if let Ok(conn) = repl_state.db_connection.get_connection_arc().lock() {
                Some(conn.get_interrupt_handle())
            } else {
                None
            }
        }
        ConnectionManager::Pipe(_) => {
            // Pipe connections don't support interrupt handles
            None
        }
        ConnectionManager::Fatboy(_) => {
            // Fatboy connections don't support interrupt handles
            None
        }
    };

    // Execute query in a separate thread (stacker grows the stack on demand).
    // Named, so a panic's incident row names its road; caught, so the
    // ledger closes with the panic's own words rather than "disconnected".
    let (tx, rx) = mpsc::channel();
    let query_thread = thread::Builder::new().name("query".to_string()).spawn(move || {
        let run_all = || -> Result<Option<crate::exec_ng::ResultMetadata>> {
        // Now we can use the cloned connection in the thread
        let result = if sql_mode {
            // For SQL mode, we'll execute directly without thread interruption for now
            // This means Ctrl-C won't work for SQL queries yet
            execute_sql_directly(
                &query_str,
                &db_connection,
                zebra_mode,
                target_stage.as_ref().or(Some(&Stage::Sql)),
            )
            .map(|_| None)
        // SQL doesn't return metadata
        } else {
            crate::exec_ng::ZEBRA_MODE.with(|z| *z.borrow_mut() = zebra_mode);

            let run = || -> Result<Option<crate::exec_ng::ResultMetadata>> {
                let mut handle = dql_handle.lock().unwrap_or_else(|e| e.into_inner());
                let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
                crate::exec_ng::execute_query(
                    &query_str,
                    &mut *session,
                    target_stage,
                    output_format,
                    no_headers,
                    false,
                    false,
                )
            };
            run()
        };
        result
        };
        let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(run_all)) {
            Ok(result) => result,
            Err(payload) => {
                let message = if let Some(s) = payload.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = payload.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "panic with non-string payload".to_string()
                };
                Err(anyhow::anyhow!(
                    "[{}] the query thread panicked: {message}",
                    crate::client::incident::PANIC_URI
                ))
            }
        };
        let _ = tx.send(result);
    })
    .expect("the query thread spawns");

    // Wait for query completion or interruption
    loop {
        // Check if interrupted
        if interrupted_flag.load(Ordering::Relaxed) {
            println!("Query execution interrupted");
            close_ledger(
                repl_state,
                crate::client::database::InputOutcome::Interrupted,
                None,
                None,
                Some(start_time.elapsed().as_secs_f64() * 1000.0),
            );

            // Interrupt the SQLite connection using the handle we got earlier
            if let Some(ref handle) = interrupt_handle {
                handle.interrupt();
            }

            // Reset the interrupted flag
            interrupted_flag.store(false, Ordering::Relaxed);

            // Wait a bit for the interrupt to take effect
            thread::sleep(Duration::from_millis(100));

            // Drop the receiver and abandon the thread
            drop(rx);
            drop(query_thread);

            // Clear any remaining interrupt state (SQLite only)
            if let Some(ref _handle) = interrupt_handle {
                // Check if still interrupted and clear it
                if let ConnectionManager::SQLite(_) = &repl_state.db_connection {
                    if let Ok(conn) = repl_state.db_connection.get_connection_arc().lock() {
                        if conn.is_interrupted() {
                            // The interrupt worked
                        }
                    }
                }
            }

            return Ok(());
        }

        // Check if query completed
        match rx.try_recv() {
            Ok(result) => {
                // Process the result
                let execution_time = start_time.elapsed();
                repl_state.last_execution_time = Some(execution_time);

                let execution_ms = execution_time.as_secs_f64() * 1000.0;
                let last_sql = repl_state.shared_info.last_sql.clone();

                match result {
                    Ok(_metadata) => {
                        close_ledger(
                            repl_state,
                            crate::client::database::InputOutcome::Succeeded,
                            None,
                            last_sql,
                            Some(execution_ms),
                        );

                        // TODO: revisit repl capture — currently chokes on (~~ddl ~~) annotations
                        // because it re-parses raw input text without grammar-aware handling.
                        // if !repl_state.sql_mode {
                        //     if let Err(e) = capture_query_as_repl_rule(query, repl_state) {
                        //         eprintln!("  (repl capture failed: {})", e);
                        //     }
                        // }

                        return Ok(());
                    }
                    Err(e) => {
                        close_ledger(
                            repl_state,
                            crate::client::database::InputOutcome::Failed,
                            Some(e.to_string()),
                            last_sql,
                            Some(execution_ms),
                        );
                        return Err(e);
                    }
                }
            }
            Err(mpsc::TryRecvError::Empty) => {
                // Query still running, sleep a bit and continue
                thread::sleep(Duration::from_millis(50));
                continue;
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                // Thread panicked or disconnected
                close_ledger(
                    repl_state,
                    crate::client::database::InputOutcome::Failed,
                    Some("query execution thread disconnected".to_string()),
                    None,
                    Some(start_time.elapsed().as_secs_f64() * 1000.0),
                );
                return Err(anyhow::anyhow!(
                    "Query execution thread disconnected unexpectedly"
                ));
            }
        }
    }
}

// Helper function to execute query and return metadata


/// Get ANSI color code based on zebra mode and column index
fn get_zebra_color(zebra_mode: Option<usize>, col_index: usize) -> &'static str {
    match zebra_mode {
        None => "", // No coloring
        Some(2) => {
            // Blue and cyan (more visible than white)
            match col_index % 2 {
                0 => "\x1b[34m", // Blue
                _ => "\x1b[36m", // Cyan
            }
        }
        Some(3) => {
            // Red, white, and blue
            match col_index % 3 {
                0 => "\x1b[31m", // Red
                1 => "\x1b[37m", // White
                _ => "\x1b[34m", // Blue
            }
        }
        Some(4) => {
            // Red, white, blue, and green
            match col_index % 4 {
                0 => "\x1b[31m", // Red
                1 => "\x1b[37m", // White
                2 => "\x1b[34m", // Blue
                _ => "\x1b[32m", // Green
            }
        }
        _ => "", // Invalid mode
    }
}

/// Reset ANSI color
const RESET_COLOR: &str = "\x1b[0m";

/// Execute SQL directly without DelightQL parsing using the persistent connection
fn execute_sql_directly(
    sql: &str,
    db_connection: &ConnectionManager,
    zebra_mode: Option<usize>,
    target_stage: Option<&crate::args::Stage>,
) -> Result<()> {
    // Execute the SQL based on connection type
    // Convert to common QueryResult type (SQLite's version)
    let results = match db_connection {
        ConnectionManager::SQLite(conn) => {
            use delightql_backends::SqliteExecutorImpl;
            let mut executor = SqliteExecutorImpl::new(conn);
            executor
                .execute_query(sql)
                .map_err(|e| anyhow::anyhow!("SQL execution error: {}", e))?
        }
        ConnectionManager::Fatboy(_) => {
            return Err(anyhow::anyhow!(
                "Direct SQL execution not supported for fatboy connections"
            ));
        }
        ConnectionManager::Pipe(_mgr) => {
            return Err(anyhow::anyhow!(
                "Direct SQL execution not supported for pipe connections"
            ));
        }
    };

    // Handle different output stages
    match target_stage {
        Some(crate::args::Stage::Hash) => {
            // Generate hash from results
            use crate::util::fingerprint::ResultFingerprint;
            use delightql_backends::QueryResults;
            use std::path::Path;

            // Convert QueryResult to QueryResults
            let query_results = QueryResults {
                columns: results.columns.clone(),
                rows: results.rows.clone(),
                row_count: results.rows.len(),
            };

            // Get the database path for fingerprinting
            let db_info = db_connection
                .connection_info()
                .map_err(|e| anyhow::anyhow!("Failed to get connection info: {}", e))?;
            let db_path_ref = db_info.path.as_deref();
            let fingerprint =
                ResultFingerprint::from_results(&query_results, db_path_ref.map(Path::new))
                    .map_err(|e| anyhow::anyhow!("Failed to generate fingerprint: {}", e))?;

            // Output just the data hash
            println!("{}", fingerprint.data_hash);
            return Ok(());
        }
        Some(crate::args::Stage::Fingerprint) => {
            // Generate full fingerprint JSON
            use crate::util::fingerprint::ResultFingerprint;
            use delightql_backends::QueryResults;
            use std::path::Path;

            // Convert QueryResult to QueryResults
            let query_results = QueryResults {
                columns: results.columns.clone(),
                rows: results.rows.clone(),
                row_count: results.rows.len(),
            };

            let db_info = db_connection
                .connection_info()
                .map_err(|e| anyhow::anyhow!("Failed to get connection info: {}", e))?;
            let db_path_ref = db_info.path.as_deref();
            let fingerprint =
                ResultFingerprint::from_results(&query_results, db_path_ref.map(Path::new))
                    .map_err(|e| anyhow::anyhow!("Failed to generate fingerprint: {}", e))?;

            let json_output = serde_json::to_string_pretty(&fingerprint)
                .map_err(|e| anyhow::anyhow!("Failed to serialize fingerprint: {}", e))?;
            println!("{}", json_output);
            return Ok(());
        }
        _ => {
            // Default: print results as table
        }
    }

    // Get row count before consuming the results
    let row_count = results.row_count();

    // Print results in table format
    if !results.columns.is_empty() {
        // Print header with zebra coloring
        let header: Vec<String> = results
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                if zebra_mode.is_some() {
                    format!("{}{}{}", get_zebra_color(zebra_mode, i), col, RESET_COLOR)
                } else {
                    col.clone()
                }
            })
            .collect();
        println!("{}", header.join("\t"));

        // Print separator with zebra coloring
        let sep: Vec<String> = results
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let dashes = "-".repeat(c.len());
                if zebra_mode.is_some() {
                    format!(
                        "{}{}{}",
                        get_zebra_color(zebra_mode, i),
                        dashes,
                        RESET_COLOR
                    )
                } else {
                    dashes
                }
            })
            .collect();
        println!("{}", sep.join("\t"));

        // Print rows with zebra coloring
        for row in results.rows {
            let colored_row: Vec<String> = row
                .iter()
                .enumerate()
                .map(|(i, val)| {
                    if zebra_mode.is_some() {
                        format!("{}{}{}", get_zebra_color(zebra_mode, i), val, RESET_COLOR)
                    } else {
                        val.clone()
                    }
                })
                .collect();
            println!("{}", colored_row.join("\t"));
        }
    }

    println!("({} rows)", row_count);

    Ok(())
}

#[cfg(test)]
mod registry_tests {
    use super::*;

    /// The weld: the dispatcher's match arms and DOT_COMMANDS must agree
    /// in BOTH directions. Arm literals are extracted from this file's
    /// own source (an arm is a line containing `=>` whose quoted strings
    /// start with '.'), so neither side can drift without failing here.
    #[test]
    fn registry_and_dispatch_agree() {
        const SRC: &str = include_str!("commands.rs");
        let mut arm_spellings = std::collections::BTreeSet::new();
        for line in SRC.lines() {
            let Some(arrow) = line.find("=>") else {
                continue;
            };
            let head = &line[..arrow];
            // Quoted literals in the arm head: ".exit" | ".quit"
            let mut parts = head.split('"');
            while let (Some(_), Some(lit)) = (parts.next(), parts.next()) {
                if lit.starts_with('.')
                    && lit.len() > 1
                    && lit[1..].chars().all(|c| c.is_ascii_lowercase() || c == '-')
                {
                    arm_spellings.insert(lit.to_string());
                }
            }
        }
        let registry: std::collections::BTreeSet<String> =
            dot_command_spellings().map(String::from).collect();

        let arms_not_registered: Vec<_> = arm_spellings.difference(&registry).collect();
        assert!(
            arms_not_registered.is_empty(),
            "dispatch arms missing from DOT_COMMANDS (help/completion/surface \
             will not know them): {:?}",
            arms_not_registered
        );
        let registered_not_arms: Vec<_> = registry.difference(&arm_spellings).collect();
        assert!(
            registered_not_arms.is_empty(),
            "DOT_COMMANDS rows with no dispatch arm (help/completion/surface \
             would advertise a command the REPL rejects): {:?}",
            registered_not_arms
        );
        assert!(!registry.is_empty(), "registry must not be empty");
    }
}

/// The prompt recovery boundary's host decision, pinned over a scripted
/// handle. What the core latch does is pinned in core (relay pump tests);
/// what is pinned HERE is the REPL's ruled behavior on top of the typed
/// report: healthy → no recovery; quarantined + reset ok → session replaced
/// and the host's database re-mounted; quarantined + reset failure →
/// terminal, never another prompt.
#[cfg(test)]
mod recovery_boundary_tests {
    use super::*;
    use delightql_core::api::{
        DqlHandle, DqlSession, FetchResult, QueryResult, ServerRelay, SessionHealthReport,
        SessionHooks, SessionRecovery,
    };
    use std::sync::{Arc, Mutex};

    struct ScriptedSession {
        queries: Arc<Mutex<Vec<String>>>,
    }

    impl DqlSession for ScriptedSession {
        fn query(&mut self, text: &str) -> Result<QueryResult, String> {
            self.queries.lock().unwrap().push(text.to_string());
            Err("scripted session: no results".to_string())
        }
        fn fetch(
            &mut self,
            _handle: &delightql_core::api::QueryHandle,
            _count: u64,
        ) -> Result<FetchResult, String> {
            Err("scripted".to_string())
        }
        fn close(&mut self, _handle: delightql_core::api::QueryHandle) -> Result<(), String> {
            Ok(())
        }
    }

    struct ScriptedHandle {
        health: SessionHealthReport,
        recover_fails: bool,
        recover_calls: Arc<Mutex<u32>>,
        queries: Arc<Mutex<Vec<String>>>,
    }

    impl DqlHandle for ScriptedHandle {
        fn session(&mut self) -> Result<Box<dyn DqlSession + '_>, String> {
            Ok(Box::new(ScriptedSession {
                queries: Arc::clone(&self.queries),
            }))
        }
        fn session_with_hooks(
            &mut self,
            _hooks: SessionHooks,
        ) -> Result<Box<dyn DqlSession + '_>, String> {
            self.session()
        }
        fn create_relay(&mut self) -> Result<Box<dyn ServerRelay + '_>, String> {
            Err("scripted".to_string())
        }
        fn session_health(&self) -> SessionHealthReport {
            self.health.clone()
        }
        fn recover_session(&mut self) -> Result<SessionRecovery, String> {
            *self.recover_calls.lock().unwrap() += 1;
            if self.recover_fails {
                Err("reset could not complete pending compensation".to_string())
            } else {
                self.health = SessionHealthReport::Healthy;
                Ok(SessionRecovery {
                    rebuilt: "the session catalog".to_string(),
                    lost: "session-local state".to_string(),
                    retained: "the connected database".to_string(),
                })
            }
        }
    }

    fn quarantined() -> SessionHealthReport {
        SessionHealthReport::Quarantined {
            operation: "created-object registration".to_string(),
            message: "registration failed".to_string(),
        }
    }

    /// Healthy session: no recovery is attempted, no reset is issued.
    #[test]
    fn a_healthy_session_needs_no_recovery() {
        let recover_calls = Arc::new(Mutex::new(0));
        let mut handle = ScriptedHandle {
            health: SessionHealthReport::Healthy,
            recover_fails: false,
            recover_calls: Arc::clone(&recover_calls),
            queries: Arc::new(Mutex::new(Vec::new())),
        };
        assert!(matches!(
            recover_repl_session(&mut handle, Some("db.sqlite")),
            ReplRecovery::NotNeeded
        ));
        assert_eq!(*recover_calls.lock().unwrap(), 0);
    }

    /// Quarantined + reset succeeds: the report carries the incident and the
    /// reset's rebuilt/lost/retained account, and the host's database is
    /// re-mounted into the replaced session.
    #[test]
    fn a_quarantined_session_is_replaced_and_the_database_remounted() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let mut handle = ScriptedHandle {
            health: quarantined(),
            recover_fails: false,
            recover_calls: Arc::new(Mutex::new(0)),
            queries: Arc::clone(&queries),
        };
        // The scripted session refuses the re-mount query (it returns no
        // results), so a real recovery over THIS handle is terminal; what the
        // no-db path pins is the decision itself.
        match recover_repl_session(&mut handle, None) {
            ReplRecovery::Recovered {
                incident,
                rebuilt,
                lost,
                retained,
            } => {
                assert!(incident.contains("created-object registration"));
                assert!(!rebuilt.is_empty());
                assert!(!lost.is_empty());
                assert!(!retained.is_empty());
            }
            _ => panic!("a successful reset over no database is a recovery"),
        }
        assert!(
            queries.lock().unwrap().is_empty(),
            "no database, no re-mount"
        );

        // With a database, the re-mount is issued into the replaced session;
        // the scripted session refuses it, and a failed re-mount is a failed
        // recovery — terminal, not a prompt over a half-replaced session.
        let queries = Arc::new(Mutex::new(Vec::new()));
        let mut handle = ScriptedHandle {
            health: quarantined(),
            recover_fails: false,
            recover_calls: Arc::new(Mutex::new(0)),
            queries: Arc::clone(&queries),
        };
        match recover_repl_session(&mut handle, Some("db.sqlite")) {
            ReplRecovery::Terminal { failure, .. } => {
                assert!(failure.contains("failed to re-mount"));
            }
            _ => panic!("a refused re-mount is a failed recovery"),
        }
        assert_eq!(
            queries.lock().unwrap().as_slice(),
            ["mount!(\"db.sqlite\", \"main\")(*)"],
            "the re-mount is the same mount the REPL opened with"
        );
    }

    /// Quarantined + reset fails: terminal. The incident and the failure both
    /// reach the report; no prompt may follow.
    #[test]
    fn a_failed_reset_is_terminal() {
        let mut handle = ScriptedHandle {
            health: quarantined(),
            recover_fails: true,
            recover_calls: Arc::new(Mutex::new(0)),
            queries: Arc::new(Mutex::new(Vec::new())),
        };
        match recover_repl_session(&mut handle, Some("db.sqlite")) {
            ReplRecovery::Terminal { incident, failure } => {
                assert!(incident.contains("created-object registration"));
                assert!(failure.contains("pending compensation"));
            }
            _ => panic!("a failed reset must be terminal"),
        }
    }
}
