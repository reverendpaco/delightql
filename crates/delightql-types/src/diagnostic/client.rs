// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `client/…` — incidents of the interactive client process.

use super::Taxon;

/// The client family. Minted by the CLI, never by the compiler; declared
/// here so the identities are typed and registered like every other.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Client))]
pub enum Client {
    /// The prompt's per-keystroke parses and the submission preflight run in
    /// a separate worker process so a parser freeze cannot take the
    /// terminal with it. This identity records the worker failing to spawn,
    /// breaking the framed protocol, or being replaced. Optional assistance
    /// (coloring, well-formedness prompts) falls back quietly; the mandatory
    /// preflight refuses the submission rather than cross an unkillable
    /// in-process parser without a verdict.
    #[leaf("worker/unavailable", class = Connection, summary = "The REPL parser containment worker could not serve.")]
    #[error("{message}")]
    WorkerUnavailable { message: String },

    /// The exact input, the operation, the entrance, the budget that
    /// applied and how containment ended it (cooperative cancel or worker
    /// kill) are one row in repl::errors.incident, deduplicated by specimen
    /// with an occurrence count. The input is retained verbatim for
    /// reproduction; `.bug` ships it.
    #[leaf("worker/budget", class = Connection, summary = "A prompt parse exceeded its containment budget.")]
    #[error("{message}")]
    WorkerBudget { message: String },

    /// The breaker for the OPTIONAL per-keystroke probes (syntax coloring,
    /// parse-aware prompts, continuation navigation) trips on the first
    /// incident from one of them. Submission preflight is mandatory and
    /// stays on. `.repl helpers on` re-enables the optional probes.
    #[leaf("assistance/disabled", class = Connection, summary = "Optional REPL parser assistance was switched off after an incident.")]
    #[error("{message}")]
    AssistanceDisabled { message: String },

    /// Every prompt submission is parsed in the containment worker first.
    /// When that parse exceeds its budget, the worker is unavailable, or the
    /// worker panics on these exact bytes, the submission is refused with
    /// its input recorded here — never handed to the in-process parser
    /// without a verdict. The compiler never saw it, so this row is the only
    /// record of the refusal.
    #[leaf("preflight/refused", class = Connection, summary = "A submission was refused before it reached the compiler.")]
    #[error("{message}")]
    PreflightRefused { message: String },

    /// The client database (repl::*) records inputs, options and incidents
    /// through a bounded pending queue when its connection is busy. A write
    /// that could neither apply nor queue is reported here rather than
    /// silently dropped; the typed in-memory value still governs behavior,
    /// only its queryable projection is missing.
    #[leaf("ledger/write_lost", class = Connection, summary = "A client-database write was lost.")]
    #[error("{message}")]
    LedgerWriteLost { message: String },

    /// The client database is mounted into every session as repl::data with
    /// fixed projections. When the mount or a projection fails, the session
    /// still serves the user's database; repl::* is unavailable until the
    /// next session reset, and this row says why.
    #[leaf("namespace/install", class = Connection, summary = "The repl::* namespace could not be installed on this session.")]
    #[error("{message}")]
    NamespaceInstall { message: String },

    /// The history file, the config directory, or a highlights file was
    /// unavailable. The prompt continues without it.
    #[leaf("config", class = Connection, summary = "A client configuration resource could not be read or written.")]
    #[error("{message}")]
    Config { message: String },

    /// The Ctrl-C handler could not be installed, a line could not be read,
    /// or the multi-pane TUI failed. Not a query error: the client's own
    /// terminal handling.
    #[leaf("terminal", class = Connection, summary = "A terminal-side failure in the interactive client.")]
    #[error("{message}")]
    Terminal { message: String },

    /// An unrecognized debug option, or an install proceeding without
    /// adapter digests: the invocation continues, and the warning is
    /// recorded against the process's argv (repl::context.argument).
    #[leaf("argument", class = Connection, summary = "A command-line argument was accepted with a warning.")]
    #[error("{message}")]
    Argument { message: String },

    /// The formatter speaks the query grammar; a definition library,
    /// unparseable input, an unhandled node, or a token-stream change all
    /// pass the input through unchanged, with exit code 2 when asked to fail
    /// on unformatted input.
    #[leaf("format", class = Connection, summary = "dql format returned the input unchanged.")]
    #[error("{message}")]
    Format { message: String },

    /// `--no-sanitize` and `-f raw` to a terminal write bytes the terminal
    /// may interpret. A deliberate choice, warned once per process and
    /// recorded so a transcript shows the terminal was exposed.
    #[leaf("sanitize/disabled", class = Connection, summary = "Output sanitization is off; terminal control sequences pass verbatim.")]
    #[error("{message}")]
    SanitizeDisabled { message: String },

    /// The in-memory SQLite engine refused to open the per-process client
    /// database. Nothing can be recorded in this process — the one failure
    /// with nowhere to land — so it is said on stderr and repl::* is
    /// unavailable.
    #[leaf("database/unavailable", class = Connection, summary = "The client database could not be created.")]
    #[error("{message}")]
    DatabaseUnavailable { message: String },

    /// `.bug <words>` stores the description as an info row in
    /// repl::errors.incident, so it travels in error.log with the incidents
    /// it describes and needs no side file.
    #[leaf("report/description", class = Connection, summary = "The words a person attached to a bug report.")]
    #[error("{message}")]
    ReportDescription { message: String },

    /// Every error should carry a delightql-error:// identity. One that
    /// reaches main() without one — or with a protocol prefix instead of a
    /// full badge — is recorded under this identity so the hole is visible
    /// in the exit log rather than invisible in stderr scrollback.
    #[leaf("unbadged", class = Connection, summary = "An error without an identity reached the process boundary.")]
    #[error("{message}")]
    Unbadged { message: String },
}
