// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The interactive client's typed configuration.
//!
//! Every field is private to this module: the typed value is the operational
//! authority, and the ONLY writers are the constructor and the typed
//! mutation operations below — a call site cannot assign a configuration
//! field, the compiler refuses. The `repl::config.option` rows and the TUI
//! snapshot are projections written FROM this state by the `ReplState`
//! operations that wrap these; no database row or snapshot field is ever an
//! authority.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::args::Stage;
use crate::output_format::OutputFormat;

/// The closed vocabulary of parser operations the worker serves. Budgets,
/// option rows, and timeout evidence all key on it — one enum, so a new
/// operation without a budget or an option row is a compile error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplParserOperation {
    PromptWellFormed,
    SyntaxHighlight,
    ContinuationNavigation,
    SubmissionPreflight,
}

impl ReplParserOperation {
    pub const ALL: [ReplParserOperation; 4] = [
        ReplParserOperation::PromptWellFormed,
        ReplParserOperation::SyntaxHighlight,
        ReplParserOperation::ContinuationNavigation,
        ReplParserOperation::SubmissionPreflight,
    ];

    /// The recorded `operation` spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            ReplParserOperation::PromptWellFormed => "prompt_well_formed",
            ReplParserOperation::SyntaxHighlight => "syntax_highlight",
            ReplParserOperation::ContinuationNavigation => "continuation_navigation",
            ReplParserOperation::SubmissionPreflight => "submission_preflight",
        }
    }

    /// The `repl::config.option` row that carries this operation's budget.
    pub fn option_name(self) -> &'static str {
        match self {
            ReplParserOperation::PromptWellFormed => "parser_budget_prompt_ms",
            ReplParserOperation::SyntaxHighlight => "parser_budget_highlight_ms",
            ReplParserOperation::ContinuationNavigation => "parser_budget_navigation_ms",
            ReplParserOperation::SubmissionPreflight => "parser_budget_preflight_ms",
        }
    }
}

/// The ruled classification of parser operations. Optional editor helpers
/// stand behind the session circuit breaker; mandatory safety operations
/// cross the worker regardless of it. The mapping in
/// [`ReplParserOperation::kind`] matches every variant by name — a new
/// operation must choose its side there or nothing compiles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplParserOperationKind {
    OptionalEditorHelper,
    MandatorySafety,
}

impl ReplParserOperation {
    /// Which side of the breaker this operation stands on.
    pub fn kind(self) -> ReplParserOperationKind {
        match self {
            ReplParserOperation::PromptWellFormed
            | ReplParserOperation::SyntaxHighlight
            | ReplParserOperation::ContinuationNavigation => {
                ReplParserOperationKind::OptionalEditorHelper
            }
            ReplParserOperation::SubmissionPreflight => ReplParserOperationKind::MandatorySafety,
        }
    }
}

/// The session's optional-helper circuit breaker: ONE shared typed authority
/// for whether the optional editor helpers (prompt well-formedness, syntax
/// highlighting, continuation navigation) may contact the parser worker.
/// `ReplConfig` performs the manual changes and the worker controller
/// performs the automatic trip on the SAME shared instance; neither side
/// holds a second boolean. Submission preflight never consults it.
pub struct ReplEditorHelperPolicy {
    enabled: AtomicBool,
}

impl ReplEditorHelperPolicy {
    /// A fresh policy with the helpers enabled — the session default.
    pub fn new_enabled() -> Arc<ReplEditorHelperPolicy> {
        Arc::new(ReplEditorHelperPolicy {
            enabled: AtomicBool::new(true),
        })
    }

    pub fn helpers_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    /// Manual control. Enabling arms the breaker again; a later incident
    /// may trip it again.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst)
    }

    /// Atomic compare-and-disable: `true` exactly when THIS call performed
    /// the enabled→disabled transition. The single-transition answer is what
    /// keeps the disable projection and message from repeating — a tripped
    /// breaker trips quietly ever after until re-armed.
    pub fn trip(&self) -> bool {
        self.enabled
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
}

/// The exhaustive operation→budget mapping. Call sites carry no duration
/// literals; they name an operation and this answers its effective budget.
#[derive(Clone, Copy, Debug)]
pub struct ReplParserBudgets {
    prompt_well_formed: Duration,
    syntax_highlight: Duration,
    continuation_navigation: Duration,
    submission_preflight: Duration,
}

impl ReplParserBudgets {
    /// Defaults chosen from focused debug-build measurements (medians over
    /// 20 runs; the handoff carries the full table): keystroke-scale inputs
    /// — valid (0.12 ms), malformed (0.57 ms), and the 46-byte freeze-cliff
    /// prefix (0.95 ms) — all parse in ~1 ms or less, so the per-keystroke
    /// probes get 25 ms: >20× headroom, still below perception when a parse
    /// wedges. Navigation is invoked per shortcut, not per keystroke, and
    /// tolerates 50 ms. Preflight runs once per submission and must admit a
    /// legitimately large paste (100 KB valid ≈ 0.9–2.0 s debug), so it
    /// gets 2 s; a 140 KB MALFORMED paste measured ~14 s and is refused —
    /// the in-process compiler would otherwise spend that time
    /// uninterruptibly.
    pub fn measured_defaults() -> ReplParserBudgets {
        ReplParserBudgets {
            prompt_well_formed: Duration::from_millis(25),
            syntax_highlight: Duration::from_millis(25),
            continuation_navigation: Duration::from_millis(50),
            submission_preflight: Duration::from_millis(2_000),
        }
    }

    /// Every operation under ONE budget — the test harness's containment
    /// probes and a future `.repl` control both want this shape.
    pub fn uniform(budget: Duration) -> ReplParserBudgets {
        ReplParserBudgets {
            prompt_well_formed: budget,
            syntax_highlight: budget,
            continuation_navigation: budget,
            submission_preflight: budget,
        }
    }

    pub fn effective(&self, operation: ReplParserOperation) -> Duration {
        match operation {
            ReplParserOperation::PromptWellFormed => self.prompt_well_formed,
            ReplParserOperation::SyntaxHighlight => self.syntax_highlight,
            ReplParserOperation::ContinuationNavigation => self.continuation_navigation,
            ReplParserOperation::SubmissionPreflight => self.submission_preflight,
        }
    }
}

/// The typed operational state. Private fields; see the module doc.
pub struct ReplConfig {
    output_format: OutputFormat,
    target_stage: Option<Stage>,
    sql_mode: bool,
    zebra_mode: Option<usize>,
    no_headers: bool,
    show_meta_output: bool,
    multiline: bool,
    parser_budgets: ReplParserBudgets,
    editor_helpers: Arc<ReplEditorHelperPolicy>,
}

impl ReplConfig {
    pub fn new(output_format: OutputFormat) -> ReplConfig {
        ReplConfig {
            output_format,
            target_stage: None,
            sql_mode: false,
            zebra_mode: None,
            no_headers: false,
            show_meta_output: true,
            multiline: true,
            parser_budgets: ReplParserBudgets::measured_defaults(),
            editor_helpers: ReplEditorHelperPolicy::new_enabled(),
        }
    }

    // --- reads ---

    pub fn output_format(&self) -> OutputFormat {
        self.output_format
    }

    pub fn target_stage(&self) -> Option<Stage> {
        self.target_stage
    }

    pub fn sql_mode(&self) -> bool {
        self.sql_mode
    }

    pub fn zebra_mode(&self) -> Option<usize> {
        self.zebra_mode
    }

    pub fn no_headers(&self) -> bool {
        self.no_headers
    }

    pub fn show_meta_output(&self) -> bool {
        self.show_meta_output
    }

    pub fn multiline(&self) -> bool {
        self.multiline
    }

    pub fn parser_budgets(&self) -> &ReplParserBudgets {
        &self.parser_budgets
    }

    /// The shared breaker instance, for wiring the worker controller onto
    /// the SAME authority this configuration mutates.
    pub fn editor_helper_policy(&self) -> &Arc<ReplEditorHelperPolicy> {
        &self.editor_helpers
    }

    pub fn editor_helpers_enabled(&self) -> bool {
        self.editor_helpers.helpers_enabled()
    }

    // --- typed mutation operations: the ONLY field writers ---

    pub(super) fn set_output_format(&mut self, format: OutputFormat) {
        self.output_format = format;
    }

    pub(super) fn set_target_stage(&mut self, stage: Option<Stage>) {
        self.target_stage = stage;
    }

    pub(super) fn set_input_mode_sql(&mut self, sql: bool) {
        self.sql_mode = sql;
    }

    /// Zebra accepts 0/1 (off) or 2..=4 colors; anything else refuses and
    /// changes nothing.
    pub(super) fn set_zebra_mode(&mut self, colors: usize) -> Result<(), String> {
        match colors {
            0 | 1 => {
                self.zebra_mode = None;
                Ok(())
            }
            2..=4 => {
                self.zebra_mode = Some(colors);
                Ok(())
            }
            other => Err(format!(
                "zebra supports 2-4 colors (0 disables), not {other}"
            )),
        }
    }

    pub(super) fn set_no_headers(&mut self, no_headers: bool) {
        self.no_headers = no_headers;
    }

    pub(super) fn set_show_meta_output(&mut self, show: bool) {
        self.show_meta_output = show;
    }

    pub(super) fn set_multiline(&mut self, multiline: bool) {
        self.multiline = multiline;
    }

    /// Manual helper control. The write crosses the shared policy — the one
    /// operational authority the worker controller also reads and trips.
    pub(super) fn set_editor_parser_helpers(&mut self, enabled: bool) {
        self.editor_helpers.set_enabled(enabled);
    }

    // --- renderings for the option projection ---

    /// The `.to` vocabulary spelling of the current stage.
    pub fn target_stage_rendered(&self) -> &'static str {
        match self.target_stage {
            None => "results",
            Some(Stage::Cst) => "cst",
            Some(Stage::AstUnresolved) => "ast-unresolved",
            Some(Stage::AstResolved) => "ast-resolved",
            Some(Stage::AstRefined) => "ast-refined",
            Some(Stage::AstSql) => "ast-sql",
            Some(Stage::Sql) => "sql",
            Some(Stage::Results) => "results",
            Some(Stage::Fingerprint) => "fingerprint",
            Some(Stage::Hash) => "hash",
            Some(Stage::ByteHash) => "bhash",
            Some(Stage::TotalHash) => "totalhash",
        }
    }

    pub fn output_format_rendered(&self) -> String {
        format!("{:?}", self.output_format).to_lowercase()
    }

    /// Every option row this configuration projects, rendered from the typed
    /// state: `(name, value, value_kind, default_value)`. Exhaustive — the
    /// projection seeds and refreshes FROM this one place.
    pub fn option_rows(&self) -> Vec<(&'static str, String, &'static str, &'static str)> {
        let mut rows = vec![
            (
                "output_format",
                self.output_format_rendered(),
                "enum",
                "table",
            ),
            (
                "target_stage",
                self.target_stage_rendered().to_string(),
                "enum",
                "results",
            ),
            (
                "input_mode",
                if self.sql_mode { "sql" } else { "dql" }.to_string(),
                "enum",
                "dql",
            ),
            (
                "zebra_columns",
                self.zebra_mode.unwrap_or(0).to_string(),
                "integer",
                "0",
            ),
            ("multiline", self.multiline.to_string(), "boolean", "true"),
            ("headers", (!self.no_headers).to_string(), "boolean", "true"),
            (
                "meta_output",
                self.show_meta_output.to_string(),
                "boolean",
                "true",
            ),
            (
                "editor_parser_helpers",
                self.editor_helpers.helpers_enabled().to_string(),
                "boolean",
                "true",
            ),
        ];
        for operation in ReplParserOperation::ALL {
            rows.push((
                operation.option_name(),
                self.parser_budgets
                    .effective(operation)
                    .as_millis()
                    .to_string(),
                "integer",
                match operation {
                    ReplParserOperation::PromptWellFormed => "25",
                    ReplParserOperation::SyntaxHighlight => "25",
                    ReplParserOperation::ContinuationNavigation => "50",
                    ReplParserOperation::SubmissionPreflight => "2000",
                },
            ));
        }
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Budget exhaustiveness: the closed operation vocabulary and the budget
    /// mapping have the same members, and every operation projects an
    /// option row.
    #[test]
    fn every_operation_has_a_budget_and_an_option_row() {
        let config = ReplConfig::new(OutputFormat::Table);
        let rows = config.option_rows();
        for operation in ReplParserOperation::ALL {
            assert!(config.parser_budgets().effective(operation) > Duration::ZERO);
            assert!(
                rows.iter()
                    .any(|(name, ..)| *name == operation.option_name()),
                "{} must project an option row",
                operation.as_str()
            );
        }
    }

    /// Configuration writer census, source half: the private fields make
    /// outside assignment a compile error; THIS pins that the only
    /// assignment sites inside the module are the constructor and the typed
    /// operations. Field inventory: output_format, target_stage, sql_mode,
    /// zebra_mode, no_headers, show_meta_output, multiline, parser_budgets.
    #[test]
    fn the_only_field_writers_are_construction_and_typed_operations() {
        const SRC: &str = include_str!("config.rs");
        for field in [
            "output_format",
            "target_stage",
            "sql_mode",
            "zebra_mode",
            "no_headers",
            "show_meta_output",
            "multiline",
            "parser_budgets",
            "editor_helpers",
        ] {
            let assignments = SRC
                .lines()
                .filter(|l| l.trim_start().starts_with(&format!("self.{field} =")))
                .count();
            let allowed = match field {
                // zebra has two lawful arms (off and colored).
                "zebra_mode" => 2,
                // budgets are construction-set until a `.repl` control lands.
                "parser_budgets" => 0,
                // the breaker mutates through the shared policy's typed
                // operations, never by field assignment.
                "editor_helpers" => 0,
                _ => 1,
            };
            assert_eq!(
                assignments, allowed,
                "field '{field}': every writer must be one typed operation"
            );
        }
    }

    /// The operation classification, pinned member by member: exactly the
    /// three editor operations are optional, and submission preflight is the
    /// one mandatory safety operation. Iterating ALL keeps the pin exhaustive
    /// when the vocabulary grows.
    #[test]
    fn the_classification_holds_exactly_three_optional_helpers() {
        let mut optional = Vec::new();
        let mut mandatory = Vec::new();
        for operation in ReplParserOperation::ALL {
            match operation.kind() {
                ReplParserOperationKind::OptionalEditorHelper => optional.push(operation.as_str()),
                ReplParserOperationKind::MandatorySafety => mandatory.push(operation.as_str()),
            }
        }
        assert_eq!(
            optional,
            [
                "prompt_well_formed",
                "syntax_highlight",
                "continuation_navigation"
            ],
            "the optional set is exactly the three editor helpers"
        );
        assert_eq!(
            mandatory,
            ["submission_preflight"],
            "submission preflight is the one mandatory safety operation"
        );
    }

    /// The breaker authority: enabled by default; the atomic
    /// compare-and-disable answers true exactly once per armed period; manual
    /// enable arms it again.
    #[test]
    fn the_policy_trips_once_per_armed_period() {
        let policy = ReplEditorHelperPolicy::new_enabled();
        assert!(policy.helpers_enabled(), "the session default is enabled");

        assert!(policy.trip(), "the first trip performs the transition");
        assert!(!policy.helpers_enabled());
        assert!(!policy.trip(), "a tripped breaker trips quietly");
        assert!(!policy.trip());

        policy.set_enabled(true);
        assert!(policy.helpers_enabled(), "manual enable re-arms");
        assert!(policy.trip(), "a re-armed breaker may trip again");
        assert!(!policy.helpers_enabled());

        policy.set_enabled(false);
        assert!(!policy.trip(), "manual off leaves nothing to trip");
    }

    /// The breaker's option row projects from the shared policy with a
    /// true default.
    #[test]
    fn the_helper_option_row_projects_the_policy() {
        let config = ReplConfig::new(OutputFormat::Table);
        let row = |config: &ReplConfig| {
            config
                .option_rows()
                .into_iter()
                .find(|(name, ..)| *name == "editor_parser_helpers")
                .expect("the breaker projects an option row")
        };
        assert_eq!(row(&config).1, "true");
        assert_eq!(row(&config).3, "true", "the default is enabled");
        config.editor_helper_policy().trip();
        assert_eq!(row(&config).1, "false", "the row renders the policy");
    }

    /// Invalid zebra values change nothing.
    #[test]
    fn invalid_zebra_values_change_none() {
        let mut config = ReplConfig::new(OutputFormat::Table);
        config.set_zebra_mode(3).unwrap();
        assert_eq!(config.zebra_mode(), Some(3));
        assert!(config.set_zebra_mode(9).is_err());
        assert_eq!(
            config.zebra_mode(),
            Some(3),
            "a refused value changes nothing"
        );
        config.set_zebra_mode(0).unwrap();
        assert_eq!(config.zebra_mode(), None);
    }
}
