// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The REPL's surface: the welcome message, the dot commands, the key
//! bindings and the examples `.help` shows, and everything else the REPL
//! says in its own words.
//!
//! The registries below and [`super::commands::DOT_COMMANDS`] are the one
//! source of this wording. They seed `repl::surface` when the client
//! database opens, and what the REPL says is rendered from what
//! `repl::surface` reads back — so what the screen says is exactly what
//! `repl::surface.*(*)` answers. Only when the client database is absent or
//! unreadable do the renderers read the registries directly.

use crate::client::database::{
    ClientDatabase, ExampleRow, KeyBindingRow, MessageRow, Surface, SurfaceRow,
};

/// The welcome message, one line each. `{version}` is the build's version
/// and `{connection}` the database the session opened on.
const WELCOME: &[&str] = &[
    "DelightQL {version} · {connection}",
    "Type .help for commands and keys, .exit to leave.",
];

/// The keys `.help` lists, as (section, keys, action).
const KEY_BINDINGS: &[(&str, &str, &str)] = &[
    (
        "Keys",
        "Enter",
        "Continue a multiline query, or run a single-line one",
    ),
    ("Keys", "Enter on an empty line", "Run the multiline query"),
    ("Keys", "Alt+Enter", "Insert a newline"),
    ("Keys", "Ctrl-C", "Discard the input being typed"),
    (
        "Keys",
        "Tab",
        "Complete a dot command, or show what the query so far publishes",
    ),
    (
        "Keys",
        "Ctrl-B / Ctrl-F",
        "Jump to the previous / next continuation",
    ),
    (
        "Keys",
        "Ctrl-X d / Ctrl-X D",
        "Delete to the next / previous continuation",
    ),
];

/// The examples `.help` lists, as (section, query, note).
const EXAMPLES: &[(&str, &str, &str)] = &[
    ("Examples", "users(*) |> (name, email)", "keep two columns"),
    (
        "Examples",
        "products(*), price > 10 ~> avg:(price)",
        "filter, then aggregate",
    ),
    (
        "Examples",
        "repl::surface.dot_command(*)",
        "these commands, as a relation",
    ),
    (
        "Examples",
        "ls(*)",
        "every name that answers bare here, and where it lives",
    ),
];

macro_rules! messages {
    ($($message:ident $name:literal => $wording:literal),+ $(,)?) => {
        /// Something the REPL says in its own words. Everything that says
        /// one matches on this, so a message added to the declaration is
        /// seeded, and one removed is a compile error where it was said.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum Message {
            $($message),+
        }

        impl Message {
            /// Every message, in the order `repl::surface.message` lists them.
            pub const ALL: &'static [Message] = &[$(Message::$message),+];

            /// The name its `repl::surface.message` row is stored under.
            pub fn name(self) -> &'static str {
                match self {
                    $(Message::$message => $name),+
                }
            }

            /// The wording the row is seeded with.
            pub fn wording(self) -> &'static str {
                match self {
                    $(Message::$message => $wording),+
                }
            }
        }
    };
}

messages! {
    UnknownCommand "unknown_command" => "Unknown command {command}. Type .help for the list.",
    Usage "usage" => "Usage: {command} {args}",
    OutputFormat "output_format" => "Output format: {format}",
    FormatChoices "format_choices" => "Formats: {formats}",
    UnknownFormat "unknown_format" => "Unknown format '{format}'. Formats: {formats}",
    OutputStage "output_stage" => "Output stage: {stage}",
    StageChoices "stage_choices" => "Stages: {stages}",
    UnknownStage "unknown_stage" => "Unknown stage '{stage}'. Stages: {stages}",
    QueryMode "query_mode" => "Query input: each submission runs as one query.",
    DdlMode "ddl_mode" => "Definition input: rules and functions land in home. .query returns.",
    SqlMode "sql_mode" => "SQL input: each submission runs as raw SQL.",
    QueryOneOff "query_one_off" => "Running as a query: {query}",
    SqlOneOff "sql_one_off" => "Running as SQL: {query}",
    Defined "defined" => "Defined {entities}.",
    Multiline "multiline" => "Multiline input: {state}",
    Helpers "helpers" => "Parser helpers (syntax coloring, parse-aware prompts, continuation navigation): {state}",
    Preflight "preflight" => "Submission safety preflight: always on",
    HelpersOn "helpers_on" => "Parser helpers on. They switch themselves off again if the parser stalls.",
    HelpersOff "helpers_off" => "Parser helpers off. Submission safety preflight stays on.",
    BugWritten "bug_written" => "Bug report: {archive}",
    BugContents "bug_contents" => "It holds {databases} database file(s), {ddl_files} DDL file(s), the REPL database and the session files.",
    BugReplay "bug_replay" => "Replay it with: dql query --replay-repl {archive}",
    QueryInterrupted "query_interrupted" => "Query interrupted.",
    CtrlC "ctrl_c" => "Ctrl-C (type .exit to leave)",
    CtrlD "ctrl_d" => "Ctrl-D",
    Goodbye "goodbye" => "Goodbye!",
}

/// The registries, as the surface they seed.
pub fn registry() -> Surface {
    let dot_commands = super::commands::DOT_COMMANDS
        .iter()
        .enumerate()
        .flat_map(|(ordinal, cmd)| {
            let row = move |spelling: &str, is_alias: bool| SurfaceRow {
                ordinal: ordinal as i64 + 1,
                spelling: spelling.to_string(),
                canonical_name: cmd.name.to_string(),
                is_alias,
                args: cmd.args.to_string(),
                section: cmd.section.to_string(),
                summary: cmd.summary.to_string(),
                example: cmd.example.to_string(),
            };
            std::iter::once(row(cmd.name, false))
                .chain(cmd.aliases.iter().map(move |alias| row(alias, true)))
        })
        .collect();
    Surface {
        dot_commands,
        welcome: WELCOME.iter().map(|line| line.to_string()).collect(),
        key_bindings: KEY_BINDINGS
            .iter()
            .enumerate()
            .map(|(ordinal, (section, keys, action))| KeyBindingRow {
                ordinal: ordinal as i64 + 1,
                section: section.to_string(),
                keys: keys.to_string(),
                action: action.to_string(),
            })
            .collect(),
        examples: EXAMPLES
            .iter()
            .enumerate()
            .map(|(ordinal, (section, query, note))| ExampleRow {
                ordinal: ordinal as i64 + 1,
                section: section.to_string(),
                query: query.to_string(),
                note: note.to_string(),
            })
            .collect(),
        messages: Message::ALL
            .iter()
            .enumerate()
            .map(|(ordinal, message)| MessageRow {
                ordinal: ordinal as i64 + 1,
                name: message.name().to_string(),
                text: message.wording().to_string(),
            })
            .collect(),
    }
}

/// The surface this session shows: `repl::surface` as it reads back. A
/// failed read is said, and the registries answer for it.
pub fn of(repl_db: Option<&ClientDatabase>) -> Surface {
    let Some(db) = repl_db else {
        return registry();
    };
    db.surface().unwrap_or_else(|e| {
        unreadable(&e);
        registry()
    })
}

/// What the REPL says as `message`, its placeholders filled in from
/// `fills`: the wording its `repl::surface.message` row holds. A failed
/// read is said, and the built-in wording answers for it.
pub fn say(repl_db: Option<&ClientDatabase>, message: Message, fills: &[(&str, &str)]) -> String {
    fill(&wording(repl_db, message), fills)
}

/// The wording `message` is said in, its placeholders unfilled — for a
/// road that fills them where the client database is out of reach.
pub fn wording(repl_db: Option<&ClientDatabase>, message: Message) -> String {
    let stored = repl_db.and_then(|db| {
        db.message(message.name()).unwrap_or_else(|e| {
            unreadable(&e);
            None
        })
    });
    stored.unwrap_or_else(|| message.wording().to_string())
}

/// `Usage: <command> <args>`, the args as `repl::surface.dot_command` holds
/// them.
pub fn usage(repl_db: Option<&ClientDatabase>, command: &str) -> String {
    let surface = of(repl_db);
    let args = surface
        .dot_commands
        .iter()
        .find(|row| row.spelling == command)
        .map(|row| row.args.as_str())
        .unwrap_or("");
    say(
        repl_db,
        Message::Usage,
        &[("command", command), ("args", args)],
    )
}

fn unreadable(e: &anyhow::Error) {
    crate::client::incident::warning(
        "terminal",
        delightql_types::diagnostic::Client::Terminal {
            message: format!("repl::surface could not be read ({e}); showing the built-in text"),
        },
    );
}

/// `text` with each `{name}` that `fills` names replaced by its value, in
/// one pass: a value is never itself searched for placeholders, and a
/// `{name}` that `fills` does not name stays as written.
pub(crate) fn fill(text: &str, fills: &[(&str, &str)]) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let named = after.find('}').and_then(|close| {
            fills
                .iter()
                .find(|(name, _)| *name == &after[..close])
                .map(|(_, value)| (close, *value))
        });
        match named {
            Some((close, value)) => {
                out.push_str(value);
                rest = &after[close + 1..];
            }
            None => {
                out.push('{');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

/// The welcome message, its placeholders filled in.
pub fn render_welcome(surface: &Surface, version: &str, connection: &str) -> String {
    let mut out = String::new();
    for line in &surface.welcome {
        out.push_str(&fill(
            line,
            &[("version", version), ("connection", connection)],
        ));
        out.push('\n');
    }
    out
}

/// The width of the first column of `.help`.
const FIRST_COLUMN: usize = 24;

/// `.help`: the dot commands by section, then the keys, then the examples,
/// each in its table's order.
pub fn render_help(surface: &Surface) -> String {
    let mut out = String::new();
    let mut current: Option<String> = None;
    let mut heading = |out: &mut String, section: &str| {
        if current.as_deref() != Some(section) {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(section);
            out.push('\n');
            current = Some(section.to_string());
        }
    };

    for command in surface.dot_commands.iter().filter(|row| !row.is_alias) {
        heading(&mut out, &command.section);
        let mut invocation = command.spelling.clone();
        for alias in surface
            .dot_commands
            .iter()
            .filter(|row| row.is_alias && row.canonical_name == command.spelling)
        {
            invocation.push_str(", ");
            invocation.push_str(&alias.spelling);
        }
        if !command.args.is_empty() {
            invocation.push(' ');
            invocation.push_str(&command.args);
        }
        entry(&mut out, &invocation, &command.summary);
        if !command.example.is_empty() {
            entry(&mut out, "", &command.example);
        }
    }
    for binding in &surface.key_bindings {
        heading(&mut out, &binding.section);
        entry(&mut out, &binding.keys, &binding.action);
    }
    // An example is a line to copy, its note a comment the line keeps.
    let widest = surface
        .examples
        .iter()
        .map(|example| example.query.chars().count())
        .max()
        .unwrap_or(0);
    for example in &surface.examples {
        heading(&mut out, &example.section);
        let pad = " ".repeat(widest - example.query.chars().count());
        out.push_str(&format!("  {}{pad}  // {}\n", example.query, example.note));
    }
    out
}

/// One two-column line; a first column too wide for its place takes a line
/// of its own.
fn entry(out: &mut String, first: &str, second: &str) {
    let width = first.chars().count();
    if width < FIRST_COLUMN {
        out.push_str(&format!(
            "  {first}{} {second}\n",
            " ".repeat(FIRST_COLUMN - width)
        ));
    } else {
        out.push_str(&format!(
            "  {first}\n  {} {second}\n",
            " ".repeat(FIRST_COLUMN)
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every command, alias, key and example the surface holds is on the
    /// help screen, under its section, in its table's order.
    #[test]
    fn help_shows_every_row_of_the_surface_in_order() {
        let surface = registry();
        let help = render_help(&surface);
        let mut at = 0;
        for needle in surface
            .dot_commands
            .iter()
            .filter(|row| !row.is_alias)
            .map(|row| row.summary.as_str())
            .chain(surface.key_bindings.iter().map(|row| row.action.as_str()))
            .chain(surface.examples.iter().map(|row| row.query.as_str()))
        {
            let found = help[at..]
                .find(needle)
                .unwrap_or_else(|| panic!("{needle:?} missing or out of order in:\n{help}"));
            at += found + needle.len();
        }
        for alias in surface.dot_commands.iter().filter(|row| row.is_alias) {
            assert!(
                help.contains(&format!("{}, {}", alias.canonical_name, alias.spelling)),
                "{} is listed beside {}",
                alias.spelling,
                alias.canonical_name
            );
        }
    }

    /// The help screen says what the rows say and nothing of its own: a
    /// reworded row is a reworded screen.
    #[test]
    fn help_is_rendered_from_the_rows_it_is_given() {
        let mut surface = registry();
        surface.dot_commands[0].summary = "A REWORDED SUMMARY".to_string();
        surface.key_bindings[0].section = "Shortcuts".to_string();
        surface.examples.truncate(1);
        let help = render_help(&surface);
        assert!(help.contains("A REWORDED SUMMARY"));
        assert!(help.contains("\nShortcuts\n"));
        assert_eq!(help.matches("  // ").count(), 1);
    }

    /// The welcome message fills in its placeholders and keeps its lines.
    #[test]
    fn the_welcome_message_fills_its_placeholders() {
        let welcome = render_welcome(&registry(), "9.9.9", "SQLite, in memory");
        assert!(welcome.contains("9.9.9"));
        assert!(welcome.contains("SQLite, in memory"));
        assert!(!welcome.contains('{'));
        assert_eq!(welcome.lines().count(), WELCOME.len());
    }

    /// A placeholder is filled once: a value is not searched for further
    /// placeholders, and a placeholder nothing fills stays as written.
    #[test]
    fn placeholders_fill_in_one_pass() {
        assert_eq!(
            fill(
                "Unknown format '{format}'. Formats: {formats}",
                &[("format", "{formats}"), ("formats", "table, box")],
            ),
            "Unknown format '{formats}'. Formats: table, box"
        );
        assert_eq!(fill("{unfilled} and {", &[]), "{unfilled} and {");
    }

    /// Every message is seeded under a name of its own, in the declaration's
    /// order, with the wording it declares.
    #[test]
    fn every_message_is_seeded_under_its_own_name() {
        let rows = registry().messages;
        assert_eq!(rows.len(), Message::ALL.len());
        let names: std::collections::BTreeSet<&str> =
            rows.iter().map(|row| row.name.as_str()).collect();
        assert_eq!(names.len(), rows.len(), "names are unique");
        for (row, message) in rows.iter().zip(Message::ALL) {
            assert_eq!(row.name, message.name());
            assert_eq!(row.text, message.wording());
        }
    }

    /// What is said is the row's wording, not the registry's: a reworded
    /// row is a reworded message.
    #[test]
    fn a_message_is_said_as_its_row_words_it() {
        use crate::client::context::{Mode, ProcessContext};
        let mut surface = registry();
        let row = surface
            .messages
            .iter_mut()
            .find(|row| row.name == Message::UnknownCommand.name())
            .unwrap();
        row.text = "No {command} here".to_string();
        let db = ClientDatabase::open(ProcessContext::capture(Mode::Other), &surface)
            .expect("open repl database");
        assert_eq!(
            say(Some(&db), Message::UnknownCommand, &[("command", ".zebra")]),
            "No .zebra here"
        );
        assert_eq!(say(Some(&db), Message::Goodbye, &[]), "Goodbye!");
    }

    /// Without a client database the built-in wording is said.
    #[test]
    fn without_a_client_database_the_built_in_wording_is_said() {
        assert_eq!(say(None, Message::Goodbye, &[]), Message::Goodbye.wording());
        assert_eq!(usage(None, ".multiline"), "Usage: .multiline [on|off]");
    }
}
