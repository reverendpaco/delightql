// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// Shared REPL command handler
use anyhow::{Context, Result};
use std::time::Instant;

use super::info_panel::SharedReplState;
use crate::args::Stage;
use crate::bug_report::SessionLog;
use crate::connection::ConnectionManager;
use crate::output_format::OutputFormat;
use crate::version_info;
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
    pub output_format: OutputFormat,
    pub last_query: Option<String>,
    pub last_execution_time: Option<std::time::Duration>,
    pub target_stage: Option<Stage>, // Which stage to output (None = Results)
    pub shared_info: SharedReplState, // Shared with multi-pane TUI
    pub sql_mode: bool,              // Whether to bypass DelightQL parsing and execute SQL directly
    pub db_connection: ConnectionManager, // Persistent database connection
    pub dql_handle: Arc<std::sync::Mutex<Box<dyn delightql_core::api::DqlHandle>>>, // Persistent DqlHandle (wrapped in Arc<Mutex> for thread-safe mutation)
    pub show_meta_output: bool, // Whether to show meta-command output (verbose/quiet control)
    pub zebra_mode: Option<usize>, // Number of colors for zebra striping columns (None = off)
    pub no_headers: bool,       // Whether to suppress headers in results output
    pub name_generator: super::name_generator::ReplNameGenerator,
    pub captures: Vec<ReplCapture>,
    pub repl_namespace_initialized: bool,
    pub session_log: SessionLog,
    pub multiline: bool, // Whether Enter accumulates lines (true) or submits immediately (false)
}

impl ReplState {
    pub fn new(db_path: Option<String>, output_format: OutputFormat) -> Result<Self> {
        Self::new_with_connection(db_path, output_format, None)
    }

    pub fn new_with_connection(
        db_path: Option<String>,
        output_format: OutputFormat,
        connection: Option<ConnectionManager>,
    ) -> Result<Self> {
        // Create DqlHandle via factory-only open
        let db_connection = if let Some(conn) = connection {
            conn
        } else if let Some(ref path) = db_path {
            ConnectionManager::new_file(path)?
        } else {
            ConnectionManager::new_memory()?
        };

        let mut handle = crate::connection::open_handle()?;

        // mount! the user database as "main" if specified
        if let Some(ref path) = db_path {
            let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
            crate::exec_ng::run_dql_query(
                &format!("mount!(\"{}\", \"main\")(*)", path),
                &mut *session,
            )?;
        }

        let dql_handle = Arc::new(std::sync::Mutex::new(handle));

        let session_log = SessionLog::new(std::env::args().collect(), db_path.clone());
        let shared_info = SharedReplState::new(std::env::args().collect(), db_path.clone());

        Ok(Self {
            db_path,
            output_format,
            last_query: None,
            last_execution_time: None,
            target_stage: None, // Default to Results
            shared_info,
            sql_mode: false, // Default to DelightQL mode
            db_connection,
            dql_handle,
            show_meta_output: true, // Default to verbose (will be overridden as needed)
            zebra_mode: None,       // Default to no zebra coloring
            no_headers: false,      // Default to showing headers
            name_generator: super::name_generator::ReplNameGenerator::new(),
            captures: Vec::new(),
            repl_namespace_initialized: false,
            session_log,
            multiline: true, // Default to multiline mode on
        })
    }

    /// Sync current config into shared_info for TUI display
    pub fn sync_shared_config(&mut self) {
        let output_format = format!("{:?}", self.output_format);
        let target_stage = match self.target_stage {
            None => "results".to_string(),
            Some(ref s) => format!("{:?}", s).to_lowercase(),
        };
        self.shared_info.sync_config(
            &output_format,
            &target_stage,
            self.sql_mode,
            self.zebra_mode,
            self.no_headers,
            self.multiline,
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
                    failure: format!("the session was reset but '{path}' failed to re-mount: {failure}"),
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
        let mut handle = repl_state.dql_handle.lock().unwrap_or_else(|e| e.into_inner());
        recover_repl_session(&mut **handle, repl_state.db_path.as_deref())
    };
    match decision {
        ReplRecovery::NotNeeded => CommandResult::Continue,
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
    DotCommand { name: ".help", aliases: &[], args: "", section: "General", summary: "Show this help message", example: "" },
    DotCommand { name: ".exit", aliases: &[".quit"], args: "", section: "General", summary: "Exit the REPL", example: "" },
    DotCommand { name: ".version", aliases: &[], args: "", section: "General", summary: "Show version information", example: "" },
    DotCommand { name: ".info", aliases: &[], args: "", section: "Display & Output", summary: "Show multi-pane TUI (Ctrl+T toggles while typing)", example: "" },
    DotCommand { name: ".format", aliases: &[], args: "[FORMAT]", section: "Display & Output", summary: "Set or show output format (table, json, csv, tsv, list)", example: "" },
    DotCommand { name: ".zebra", aliases: &[], args: "[0-4]", section: "Display & Output", summary: "Column coloring (0=off [default], 2=blue/cyan, 3=RWB, 4=RWBG)", example: "" },
    DotCommand { name: ".to", aliases: &[], args: "[STAGE]", section: "Display & Output", summary: "Show output stage (cst, ast-unresolved, ast-resolved, etc.)", example: "" },
    DotCommand { name: ".attach", aliases: &[], args: "'path' to \"namespace\"", section: "Database Operations", summary: "Attach external database and import entities", example: ".attach 'nba.db' to \"sports::nba\"" },
    DotCommand { name: ".enlist", aliases: &[], args: "<namespace> [in <target>]", section: "Database Operations", summary: "Enlist namespace entities (default target: main)", example: ".enlist import::nba" },
    DotCommand { name: ".delist", aliases: &[], args: "<namespace> [from <target>]", section: "Database Operations", summary: "Delist namespace (remove from scope)", example: ".delist import::nba" },
    DotCommand { name: ".dql", aliases: &[], args: "[query]", section: "Mode Commands", summary: "Switch to DQL mode (default); with a query, execute one-off", example: "" },
    DotCommand { name: ".sql", aliases: &[], args: "[query]", section: "Mode Commands", summary: "Switch to SQL mode; with a query, execute one-off", example: "" },
    DotCommand { name: ".multiline", aliases: &[], args: "[on|off]", section: "Mode Commands", summary: "Toggle multiline input mode (default: on)", example: "" },
    DotCommand { name: ".file", aliases: &[], args: "<path>", section: "File & Diagnostics", summary: "Execute queries from a file", example: "" },
    DotCommand { name: ".bug", aliases: &[], args: "", section: "File & Diagnostics", summary: "Create a bug report tarball from this session", example: "" },
];

/// Every spelling the dispatcher accepts (names + aliases), in registry
/// order — the projection consumed by tab completion and the surface rows.
pub fn dot_command_spellings() -> impl Iterator<Item = &'static str> {
    DOT_COMMANDS
        .iter()
        .flat_map(|c| std::iter::once(c.name).chain(c.aliases.iter().copied()))
}

/// Handle dot commands
pub fn handle_dot_command(cmd: &str, repl_state: &mut ReplState) -> Result<CommandResult> {
    let cmd = cmd.trim();
    let parts: Vec<&str> = cmd.split_whitespace().collect();

    if parts.is_empty() {
        return Ok(CommandResult::Continue);
    }

    // Log every dot command to the session log
    repl_state.session_log.log_dot_command(cmd);

    match parts[0] {
        ".exit" | ".quit" => Ok(CommandResult::Exit),

        ".info" => {
            // Switch to multi-pane TUI
            repl_state.sync_shared_config();
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

        ".version" => {
            println!("{}", version_info::get_version_info());
            Ok(CommandResult::Continue)
        }

        ".format" => {
            if parts.len() > 1 {
                match OutputFormat::from_str(parts[1]) {
                    Some(format) => {
                        repl_state.output_format = format;
                        if repl_state.show_meta_output {
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
            } else if repl_state.show_meta_output {
                println!("Current output format: {:?}", repl_state.output_format);
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
                if repl_state.show_meta_output {
                    println!("Executing SQL: {}", sql_query);
                }
                execute_sql_directly(
                    sql_query,
                    &repl_state.db_connection,
                    repl_state.zebra_mode,
                    repl_state.target_stage.as_ref(),
                )?;
            } else {
                // Set SQL mode explicitly
                repl_state.sql_mode = true;
                if repl_state.show_meta_output {
                    println!("SQL mode enabled - queries will be executed as raw SQL");
                }
            }
            Ok(CommandResult::Continue)
        }

        ".dql" => {
            if parts.len() > 1 {
                // Execute one-off DQL query while staying in current mode
                let dql_query = cmd[4..].trim(); // Skip ".dql" prefix
                if repl_state.show_meta_output {
                    println!("Executing DQL: {}", dql_query);
                }
                // Temporarily execute in DQL mode
                let saved_mode = repl_state.sql_mode;
                repl_state.sql_mode = false;
                // Note: For now we pass a dummy flag, will need to pass the global one
                let dummy_flag = std::sync::atomic::AtomicBool::new(false);
                let result = process_query(dql_query, repl_state, &dummy_flag);
                repl_state.sql_mode = saved_mode;
                result?;
            } else {
                // Set DQL mode explicitly
                repl_state.sql_mode = false;
                if repl_state.show_meta_output {
                    println!("DQL mode enabled - queries will be parsed as DelightQL");
                }
            }
            Ok(CommandResult::Continue)
        }

        ".file" => {
            if parts.len() > 1 {
                let file_path = parts[1..].join(" "); // Handle file paths with spaces
                execute_file(&file_path, repl_state)?;
            } else {
                eprintln!("Usage: .file <path>");
                eprintln!("Example: .file queries/my_query.dql");
            }
            Ok(CommandResult::Continue)
        }

        ".zebra" => {
            if parts.len() > 1 {
                match parts[1].parse::<usize>() {
                    Ok(0) | Ok(1) => {
                        // 0 or 1 means turn off zebra mode
                        repl_state.zebra_mode = None;
                        if repl_state.show_meta_output {
                            println!("Zebra mode disabled");
                        }
                    }
                    Ok(n) if (2..=4).contains(&n) => {
                        repl_state.zebra_mode = Some(n);
                        if repl_state.show_meta_output {
                            let color_desc = match n {
                                2 => "blue and cyan",
                                3 => "red, white, and blue",
                                4 => "red, white, blue, and green",
                                _ => unreachable!(),
                            };
                            println!("Zebra mode enabled with {} colors: {}", n, color_desc);
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
                if repl_state.show_meta_output {
                    match repl_state.zebra_mode {
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

        ".attach" => {
            // Delegate to attach module — routes through session protocol via mount!()
            match crate::attach::handle_attach_command(
                cmd,
                &mut **repl_state.dql_handle.lock().unwrap(),
            ) {
                Ok(true) => {
                    // Command was handled successfully
                    Ok(CommandResult::Continue)
                }
                Ok(false) => {
                    // Not an attach command (shouldn't happen since we checked)
                    eprintln!("Internal error: attach command not recognized");
                    Ok(CommandResult::Continue)
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    Ok(CommandResult::Continue)
                }
            }
        }

        ".enlist" => match handle_enlist_command(cmd, repl_state) {
            Ok(()) => Ok(CommandResult::Continue),
            Err(e) => {
                eprintln!("Error: {}", e);
                Ok(CommandResult::Continue)
            }
        },

        ".delist" => match handle_delist_command(cmd, repl_state) {
            Ok(()) => Ok(CommandResult::Continue),
            Err(e) => {
                eprintln!("Error: {}", e);
                Ok(CommandResult::Continue)
            }
        },

        ".to" => {
            if parts.len() > 1 {
                // Parse the stage
                let stage_str = parts[1];
                match stage_str {
                    "cst" => {
                        repl_state.target_stage = Some(Stage::Cst);
                        if repl_state.show_meta_output {
                            println!("Output stage set to: CST");
                        }
                    }
                    "ast-unresolved" => {
                        repl_state.target_stage = Some(Stage::AstUnresolved);
                        if repl_state.show_meta_output {
                            println!("Output stage set to: Unresolved AST");
                        }
                    }
                    "ast-resolved" => {
                        repl_state.target_stage = Some(Stage::AstResolved);
                        if repl_state.show_meta_output {
                            println!("Output stage set to: Resolved AST");
                        }
                    }
                    "ast-refined" => {
                        repl_state.target_stage = Some(Stage::AstRefined);
                        if repl_state.show_meta_output {
                            println!("Output stage set to: Refined AST");
                        }
                    }
                    "ast-sql" | "sql-ast" => {
                        repl_state.target_stage = Some(Stage::AstSql);
                        if repl_state.show_meta_output {
                            println!("Output stage set to: SQL AST");
                        }
                    }
                    "sql" => {
                        repl_state.target_stage = Some(Stage::Sql);
                        if repl_state.show_meta_output {
                            println!("Output stage set to: SQL");
                        }
                    }
                    "results" => {
                        repl_state.target_stage = None; // None means Results
                        if repl_state.show_meta_output {
                            println!("Output stage set to: Results (default)");
                        }
                    }
                    "hash" => {
                        repl_state.target_stage = Some(Stage::Hash);
                        if repl_state.show_meta_output {
                            println!("Output stage set to: Hash");
                        }
                    }
                    "fingerprint" => {
                        repl_state.target_stage = Some(Stage::Fingerprint);
                        if repl_state.show_meta_output {
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
                if repl_state.show_meta_output {
                    match repl_state.target_stage {
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
            handle_bug_command(repl_state)?;
            Ok(CommandResult::Continue)
        }

        ".multiline" => {
            if parts.len() > 1 {
                match parts[1] {
                    "on" => repl_state.multiline = true,
                    "off" => repl_state.multiline = false,
                    _ => eprintln!("Usage: .multiline [on|off]"),
                }
            } else {
                repl_state.multiline = !repl_state.multiline;
            }
            if repl_state.show_meta_output {
                println!(
                    "Multiline mode: {}",
                    if repl_state.multiline { "on" } else { "off" }
                );
            }
            Ok(CommandResult::Continue)
        }

        _ => {
            eprintln!("Unknown command: {}", parts[0]);
            eprintln!("Type '.help' for available commands");
            Ok(CommandResult::Continue)
        }
    }
}

/// Handle .enlist command - enlist a namespace (route through session protocol)
///
/// Syntax: .enlist <namespace>
///
/// Example:
///   .enlist import::nba           # Enlists import::nba into main
fn handle_enlist_command(cmd: &str, repl_state: &ReplState) -> Result<()> {
    let parts: Vec<&str> = cmd.split_whitespace().collect();

    if parts.len() < 2 {
        anyhow::bail!("Usage: .enlist <namespace>\nExample: .enlist import::nba");
    }

    if parts.len() > 2 {
        anyhow::bail!("Usage: .enlist <namespace>\nExample: .enlist import::nba");
    }

    let namespace = parts[1].trim_matches('"');

    // Route through the session protocol — enlist!() is a DQL pseudo-predicate
    // handled by the effect executor. A higher-order directive writes both
    // groups: `(arguments)(receipt access)`. A lone group is receipt access by
    // position, so dropping the `(*)` binds zero arguments and refuses on arity.
    let dql = format!("enlist!(\"{}\")(*)", namespace);
    let mut handle = repl_state.dql_handle.lock().unwrap();
    let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
    crate::exec_ng::run_dql_query(&dql, &mut *session)?;

    println!("✓ Enlisted {} into main", namespace);
    println!(
        "  Entities from {} are now accessible without namespace prefix",
        namespace
    );

    Ok(())
}

/// Handle .delist command - delist a namespace (route through session protocol)
///
/// Syntax: .delist <namespace>
///
/// Example:
///   .delist import::nba            # Delists import::nba from main
fn handle_delist_command(cmd: &str, repl_state: &ReplState) -> Result<()> {
    let parts_vec: Vec<&str> = cmd.split_whitespace().collect();

    if parts_vec.len() < 2 {
        anyhow::bail!("Usage: .delist <namespace>\nExample: .delist import::nba");
    }

    if parts_vec.len() > 2 {
        anyhow::bail!("Usage: .delist <namespace>\nExample: .delist import::nba");
    }

    let namespace = parts_vec[1].trim_matches('"');

    // Route through the session protocol — delist!() is a DQL pseudo-predicate
    // handled by the effect executor. A higher-order directive writes both
    // groups: `(arguments)(receipt access)`. A lone group is receipt access by
    // position, so dropping the `(*)` binds zero arguments and refuses on arity.
    let dql = format!("delist!(\"{}\")(*)", namespace);
    let mut handle = repl_state.dql_handle.lock().unwrap();
    let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
    crate::exec_ng::run_dql_query(&dql, &mut *session)?;

    println!("✓ Delisted {} from main", namespace);

    Ok(())
}

/// Handle .bug command — create a bug report tarball from the current session
fn handle_bug_command(repl_state: &mut ReplState) -> Result<()> {
    use std::io::{self, BufRead};

    // Prompt for description
    println!("Bug description (end with empty line):");
    let stdin = io::stdin();
    let mut description_lines = Vec::new();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.is_empty() {
            break;
        }
        description_lines.push(line);
    }
    let description = description_lines.join("\n");

    // Prompt for title
    println!("One-word title:");
    let mut title = String::new();
    io::stdin().read_line(&mut title)?;
    let title = title.trim().to_string();

    if title.is_empty() {
        eprintln!("Title cannot be empty");
        return Ok(());
    }

    // Sanitize title for filesystem
    let title: String = title
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    let db_type = repl_state.db_connection.database_type();

    // Gather resources through the session protocol — query the cartridge table
    // via DQL instead of directly accessing the bootstrap connection.
    let (ddl_files, db_files) = {
        let mut handle = repl_state.dql_handle.lock().unwrap();
        let dql = "sys::cartridges.cartridge(*) |> (source_uri, source_type_enum)";
        let query_result = {
            let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
            crate::exec_ng::run_dql_query(dql, &mut *session)
        };
        match query_result {
            Ok(results) => {
                // Find column indices
                let uri_col = results
                    .columns
                    .iter()
                    .position(|c| c == "source_uri")
                    .unwrap_or(0);
                let kind_col = results
                    .columns
                    .iter()
                    .position(|c| c == "source_type_enum")
                    .unwrap_or(1);
                crate::bug_report::gather_resources_from_rows(
                    &results.rows,
                    uri_col,
                    kind_col,
                    &repl_state.db_path,
                )
            }
            Err(_) => {
                // Fallback: just include the primary database if available
                let mut db_files = Vec::new();
                if let Some(ref p) = repl_state.db_path {
                    let primary = std::path::PathBuf::from(p);
                    if primary.exists() {
                        db_files.push(primary);
                    }
                }
                (Vec::new(), db_files)
            }
        }
    };

    match crate::bug_report::create_bug_tarball(
        &title,
        &description,
        &repl_state.session_log,
        ddl_files,
        db_files,
        db_type,
    ) {
        Ok(path) => println!("Bug report saved to: {}", path),
        Err(e) => eprintln!("Failed to create bug report: {}", e),
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
    println!("  Ctrl+T             Toggle multi-pane TUI (H/J/K/L to navigate)");
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

/// Process a query using the new pipeline
pub fn process_query(
    query: &str,
    repl_state: &mut ReplState,
    interrupted_flag: &std::sync::atomic::AtomicBool,
) -> Result<()> {
    use std::sync::{atomic::Ordering, mpsc};
    use std::thread;
    use std::time::Duration;

    let start_time = Instant::now();

    // Store the query for session log
    repl_state.last_query = Some(query.to_string());

    // Update shared info for multi-pane TUI
    repl_state.shared_info.update(query, None);

    // Reset the interrupted flag before starting
    interrupted_flag.store(false, Ordering::Relaxed);

    // Clone what we need for the thread
    let query_str = query.to_string();
    let target_stage = repl_state.target_stage;
    let output_format = repl_state.output_format;
    let sql_mode = repl_state.sql_mode;
    let db_connection = repl_state.db_connection.clone(); // Clone the connection
    let zebra_mode = repl_state.zebra_mode; // Copy zebra mode
    let no_headers = repl_state.no_headers; // Copy no_headers option
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

    // Execute query in a separate thread (stacker grows the stack on demand)
    let (tx, rx) = mpsc::channel();
    let query_thread = thread::spawn(move || {
        // Now we can use the cloned connection in the thread
        let result = if sql_mode {
            // For SQL mode, we'll execute directly without thread interruption for now
            // This means Ctrl-C won't work for SQL queries yet
            execute_sql_directly(&query_str, &db_connection, zebra_mode, Some(&Stage::Sql))
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
        let _ = tx.send(result);
    });

    // Wait for query completion or interruption
    loop {
        // Check if interrupted
        if interrupted_flag.load(Ordering::Relaxed) {
            println!("Query execution interrupted");

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
                        // Push to query history for TUI Window C
                        if let Some(ref sql) = last_sql {
                            repl_state
                                .shared_info
                                .push_history(query.to_string(), sql.clone());
                        }

                        repl_state
                            .session_log
                            .log_query(query, last_sql, Some(execution_ms), None);

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
                        repl_state.session_log.log_query(
                            query,
                            last_sql,
                            Some(execution_ms),
                            Some(e.to_string()),
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
                return Err(anyhow::anyhow!(
                    "Query execution thread disconnected unexpectedly"
                ));
            }
        }
    }
}

// Helper function to execute query and return metadata

/// Execute queries from a file
fn execute_file(file_path: &str, repl_state: &mut ReplState) -> Result<()> {
    use std::fs;
    use std::path::Path;

    let path = Path::new(file_path);

    // Check if file exists
    if !path.exists() {
        return Err(anyhow::anyhow!("File not found: {}", file_path));
    }

    // Read the file contents
    let contents =
        fs::read_to_string(path).with_context(|| format!("Failed to read file: {}", file_path))?;

    if repl_state.show_meta_output {
        println!("Executing queries from: {}", file_path);
    }

    // Determine if it's SQL or DQL based on file extension or current mode
    let is_sql = if path.extension().and_then(|s| s.to_str()) == Some("sql") {
        true
    } else if path.extension().and_then(|s| s.to_str()) == Some("dql") {
        false
    } else {
        // Use current mode if extension is ambiguous
        repl_state.sql_mode
    };

    // Split contents into individual queries (by semicolon or double newline for DQL)
    let queries = if is_sql {
        // SQL: split by semicolon
        contents
            .split(';')
            .map(|q| q.trim())
            .filter(|q| !q.is_empty())
            .collect::<Vec<_>>()
    } else {
        // DQL: each query is typically on its own line or separated by blank lines
        contents
            .split("\n\n")
            .map(|q| q.trim())
            .filter(|q| !q.is_empty())
            .collect::<Vec<_>>()
    };

    // Execute each query
    let mut executed_count = 0;
    let mut error_count = 0;

    for (i, query) in queries.iter().enumerate() {
        // Skip pure comment queries
        if query
            .lines()
            .all(|line| line.trim().is_empty() || line.trim().starts_with("--"))
        {
            continue;
        }

        if repl_state.show_meta_output && queries.len() > 1 {
            println!("\n--- Query {} of {} ---", i + 1, queries.len());
        }

        // Execute the query
        let result = if is_sql {
            execute_sql_directly(
                query,
                &repl_state.db_connection,
                repl_state.zebra_mode,
                Some(&Stage::Sql),
            )
        } else {
            // For DQL, use the process_query function
            let dummy_flag = std::sync::atomic::AtomicBool::new(false);
            process_query(query, repl_state, &dummy_flag)
        };

        match result {
            Ok(_) => executed_count += 1,
            Err(e) => {
                error_count += 1;
                eprintln!("Error in query {}: {}", i + 1, e);
                // Continue with next query instead of failing completely
            }
        }
    }

    if repl_state.show_meta_output {
        println!("\nExecuted {} queries successfully", executed_count);
        if error_count > 0 {
            println!("{} queries failed", error_count);
        }
    }

    Ok(())
}

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
            let Some(arrow) = line.find("=>") else { continue };
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
        SessionRecovery, SessionHooks,
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
        assert!(queries.lock().unwrap().is_empty(), "no database, no re-mount");

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
