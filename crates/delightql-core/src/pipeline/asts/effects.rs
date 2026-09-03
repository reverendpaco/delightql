// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The effect AST family.
//!
//! Effect rules are user-defined directives (`name!(*) :- body`). Like the
//! DDL AST (`asts/ddl.rs`), the effect family is EPHEMERAL: built from
//! consulted definition text for validation and registration, then discarded.
//! The database stores text; ASTs are re-parsed on demand.
//!
//! This module owns:
//! - the directive CATEGORY taxonomy — the single source
//!   of truth for liminal eligibility and R9 positional checks;
//! - the typed shapes of the new constructs: effect rule (head, clauses),
//!   effect body (effect-CTE definitions + body expression), directive
//!   invocation (name, category, params, access), liminal directive;
//! - the demand walker used by the R1/R2/R4/R6/R9 validators in `system.rs`
//!   (`validate_effect_rule_discipline` — the RULE 2 precedent's sibling).
//!
//! The signed witness (`+-`) is chain structure — `Continuation::
//! SignedWitness` — a plain-pipeline citizen (resolver + transformer)
//! as well as a value-position lowering in the effect transformer.

use super::core::definitions::HoParam;
#[cfg(test)]
use super::core::Continuation;
use super::core::{
    Access, Chain, CteBinding, DomainExpression, GroundMention, PipeOp, Query, Relation,
    TruthExpression, Unresolved,
};
use super::ddl::{DdlBody, DefinitionGroup, Head};
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_visit::{
    walk_visit_access, walk_visit_boolean, walk_visit_domain, walk_visit_operator,
    walk_visit_query, walk_visit_relational, AstVisit, Descent,
};
#[cfg(test)]
use crate::pipeline::asts::core::{Comparison, Existence, GroundForm, RelationalMembership, Step};
#[cfg(test)]
use crate::pipeline::asts::core::{Polarity, Probe, ProbeAddressing};
use std::collections::HashMap;

// ============================================================================
// Directive categories
// ============================================================================

/// The category of a directive, by what it directs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectiveCategory {
    /// Directs the session's namespace tree; liminal-eligible.
    Session,
    /// Creates database objects.
    Ddl,
    /// Writes rows in user tables, with the mutation verb the name means.
    Dml(crate::names::DmlVerb),
    /// Starts runs (`run!`, `run_namespace!`).
    Execution,
    /// Directs the run itself (stop, return, sequence, print).
    Utility,
    /// A user directive — defined by an effect rule (or unknown; the
    /// distinction is resolution's, not the category taxonomy's).
    User,
}

// ============================================================================
// The authoritative directive descriptor
// ============================================================================

/// How a built-in directive is realized by the implementation. Every
/// intentional contextual absence is a POLICY here, never a missing
/// registration accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectiveRealization {
    /// A registered bin entity, invocable as a pseudo-predicate through
    /// namespace-aware registry identity.
    Entity,
    /// A syntax pipe terminal (`source |> name!(args)(*)`): it has no
    /// callable entity because its meaning requires the piped input
    /// relation. Direct pseudo-predicate invocation refuses by policy.
    SyntaxPipeTerminal,
    /// Legal only in the liminal space of a consulted file; there is no
    /// callable entity by policy.
    LiminalOnly,
}

/// A typed higher-order parameter in a directive's descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectiveParam {
    pub name: &'static str,
    /// Today every registered built-in binds string/path arguments; the
    /// kind field exists so relation-target and relational parameters
    /// extend the descriptor rather than bypass it.
    pub kind: DirectiveParamKind,
    pub optional: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectiveParamKind {
    /// A string literal or bare `::`-qualified namespace path.
    StringOrPath,
    /// A namespace-positioned parameter: in the liminal space
    /// its argument takes `.::`/`::` prefix resolution relative to the
    /// consulting namespace — the ONE piece of liminal argument policy,
    /// declared here instead of hand-spelled per directive arm.
    Namespace,
    /// A whole-table DESIGNATOR naming where the effect lands. THE TARGET IS
    /// A PARAMETER: a creation or insertion target is an ordinary argument,
    /// so it occupies a parameter position and its group is the argument
    /// group — never the receipt access.
    RelationTarget,
    /// A closed pure rule value. Construction judges the authored designator
    /// through the ordinary residual-value authority; the descriptor does
    /// not assign callable identity or re-resolve its spelling.
    RuleValue,
}

/// What a directive packages in its receipt's `returned` interior relation.
/// `None` means the receipt declares
/// no payload — unwrapping it with `!>` or `.returned(*)` is a category
/// error taught as such.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptPayload {
    /// No `returned` payload declared (deliberately preserved option).
    None,
    /// Packages its input relation (heading = HO parameter 1's heading).
    Input,
    /// Packages its OTHER relational parameter (`returning_other!`).
    OtherRelation,
    /// Packages a produced collection (e.g. `run!`'s result table).
    RunResult,
    /// Packages the namespaces the operation established (`consult!`,
    /// `mount_tree!` — one row per created sub-namespace).
    Namespaces,
    /// Packages the consulted files (`consult_tree!`): one row per file,
    /// `⟦path, namespace, definitions⟧`.
    ConsultedFiles,
    /// Packages the manifest entities the operation materialized
    /// (`imprint!`/`imprint_replace!`): one row per entity,
    /// `⟦entity, status⟧`.
    MaterializedEntities,
    /// `assert!` packages both the witness relation used for its verdict and
    /// the exact input occurrence it releases.
    Assertion,
}

/// One declared flat echo column in a directive's receipt: a scalar column
/// after the guaranteed
/// `(success, operation)` core. An OPTIONAL echo is always present in the
/// heading and carries NULL when the call form omits it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiptEcho {
    pub name: &'static str,
    pub optional: bool,
}

/// One built-in directive's authoritative descriptor. Catalog
/// synchronization, argument binding, contextual refusal, and receipt
/// construction derive from this record; entity-local metadata must agree
/// (enforced by `descriptor_agreement` unit tests) and is never a second
/// authority.
#[derive(Debug, Clone, Copy)]
pub struct DirectiveDescriptor {
    /// Bare name, no `!` suffix.
    pub name: &'static str,
    pub category: DirectiveCategory,
    /// Catalog identity namespace (fully qualified).
    pub namespace: &'static str,
    pub realization: DirectiveRealization,
    /// Typed authored parameters. A syntax terminal's piped source is not
    /// counted here; its creation or mutation target is.
    pub params: &'static [DirectiveParam],
    /// The declared flat echo columns after the `(success, operation)`
    /// core, in ledger order. Echo names are
    /// ruled per directive and need not mirror parameter names
    /// (`ground!`'s params are `data_ns/lib_ns/new_ns_name`; its echoes
    /// are `data_namespace/lib_namespace/namespace`).
    pub receipt_echoes: &'static [ReceiptEcho],
    /// Whether the receipt carries the interior `input` echo of the
    /// lifted argument table (`consult!`, `doc!`).
    pub receipt_input_echo: bool,
    pub receipt_payload: ReceiptPayload,
    /// Side-effect character (compile is the notable pure entity).
    pub side_effects: bool,
}

impl DirectiveDescriptor {
    /// The receipt heading this descriptor declares, as `(name, type)`
    /// columns: the guaranteed core, then the flat echoes, then the
    /// interior additions in ruled order (`input` before `returned`).
    /// This is THE source entities' output schemas and the transformer's
    /// receipt shapes derive from — never a second copy beside it.
    pub fn receipt_columns(&self) -> Vec<(String, String)> {
        let mut cols = vec![
            ("success".to_string(), "Integer".to_string()),
            ("operation".to_string(), "String".to_string()),
        ];
        for e in self.receipt_echoes {
            cols.push((e.name.to_string(), "String".to_string()));
        }
        if self.receipt_input_echo {
            cols.push(("input".to_string(), "Interior".to_string()));
        }
        match self.receipt_payload {
            ReceiptPayload::None => {}
            ReceiptPayload::Assertion => {
                cols.push(("witnesses".to_string(), "Interior".to_string()));
                cols.push(("returned".to_string(), "Interior".to_string()));
            }
            _ => cols.push(("returned".to_string(), "Interior".to_string())),
        }
        cols
    }

    /// Judge the authored groups of one built-in invocation from this
    /// descriptor. Parenthesis position has already decided which group is
    /// arguments and which is receipt access; the descriptor supplies the
    /// parameter roles and the receipt's exact shape.
    pub fn judge_invocation(
        &self,
        authored_arguments: usize,
        piped: bool,
        access: &Access,
    ) -> Result<()> {
        self.judge_receipt_access(access)?;

        // An entity invocation written at ground supplies every parameter in
        // its argument group. A syntax DDL/DML terminal's pipe supplies its
        // source, not its declared target parameter, so its authored group is
        // likewise exact. Piped entities bind their parameter values from the
        // source relation and are checked by the lifted binder instead.
        if (!piped && self.realization == DirectiveRealization::Entity)
            || matches!(
                self.category,
                DirectiveCategory::Ddl | DirectiveCategory::Dml(_)
            )
        {
            self.judge_argument_arity(authored_arguments)?;
        }
        Ok(())
    }

    /// Judge the ordinary argument arity declared by this descriptor.
    pub fn judge_argument_arity(&self, supplied: usize) -> Result<()> {
        let required = self.params.iter().filter(|param| !param.optional).count();
        let maximum = self.params.len();
        if supplied >= required && supplied <= maximum {
            return Ok(());
        }
        let expectation = if required == maximum {
            required.to_string()
        } else {
            format!("{required}..{maximum}")
        };
        Err(DelightQLError::validation_error_categorized(
            "directive/binding/arity",
            format!(
                "{}! expects {expectation} argument(s) ({}), got {supplied}",
                self.name,
                self.params
                    .iter()
                    .map(|param| param.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            "directive arity",
        ))
    }

    /// Judge receipt access against the descriptor's declared heading.
    /// This runs before argument arity so a one-group higher-order call says
    /// truthfully that its visible group was read as receipt access.
    pub fn judge_receipt_access(&self, access: &Access) -> Result<()> {
        if access.is_whole() {
            return Ok(());
        }
        let declared = self.receipt_columns();
        let Some(binders) = access.binders() else {
            return Err(DelightQLError::validation_error_categorized(
                "directive/invocation/access",
                format!(
                    "the visible group on {}! is RECEIPT access, not its ordinary arguments; \
                     receipt access must be a positional list of plain names with {} column(s) \
                     ({}) or (*). Write the argument group first: {}!(arguments)(*)",
                    self.name,
                    declared.len(),
                    declared
                        .iter()
                        .map(|(name, _)| name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    self.name,
                ),
                "receipt access",
            ));
        };
        if binders.len() == declared.len() {
            return Ok(());
        }
        Err(DelightQLError::validation_error_categorized(
            "directive/invocation/access",
            format!(
                "receipt access on {}! binds {} column(s) but the declared receipt has {} ({}) \
                 — positional binding requires the exact arity, or (*) for the whole receipt",
                self.name,
                binders.len(),
                declared.len(),
                declared
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            "receipt access",
        ))
    }

    /// Whether this directive is an AD-HOC STATEMENT TERMINAL: its meaning is
    /// realized by the typed effect plan and requires the relation a pipe
    /// hands it.
    ///
    /// The realization says its meaning REQUIRES the piped input, so there is
    /// no callable entity to invoke instead. Every such directive needs the
    /// statement road rather than the entity road, and the relay asks this rather than
    /// keeping a list of the names that answer it today — declaring one more
    /// reaches the routing, and changing a realization retires it.
    ///
    /// `imprint!` is DDL realized as an ENTITY and answers no.
    pub fn is_adhoc_statement_terminal(&self) -> bool {
        (matches!(
            self.category,
            DirectiveCategory::Ddl | DirectiveCategory::Dml(_)
        ) || matches!(self.name, "assert" | "abort"))
            && matches!(self.realization, DirectiveRealization::SyntaxPipeTerminal)
    }
}

const fn p(name: &'static str) -> DirectiveParam {
    DirectiveParam {
        name,
        kind: DirectiveParamKind::StringOrPath,
        optional: false,
    }
}

const fn pt(name: &'static str) -> DirectiveParam {
    DirectiveParam {
        name,
        kind: DirectiveParamKind::RelationTarget,
        optional: false,
    }
}

const fn pn(name: &'static str) -> DirectiveParam {
    DirectiveParam {
        name,
        kind: DirectiveParamKind::Namespace,
        optional: false,
    }
}

const fn pr(name: &'static str) -> DirectiveParam {
    DirectiveParam {
        name,
        kind: DirectiveParamKind::RuleValue,
        optional: false,
    }
}

const fn po(name: &'static str) -> DirectiveParam {
    DirectiveParam {
        name,
        kind: DirectiveParamKind::StringOrPath,
        optional: true,
    }
}

const fn e(name: &'static str) -> ReceiptEcho {
    ReceiptEcho {
        name,
        optional: false,
    }
}

const fn eo(name: &'static str) -> ReceiptEcho {
    ReceiptEcho {
        name,
        optional: true,
    }
}

const STD_PRELUDE: &str = "std::prelude";

// ============================================================================
// THE ONE DECLARATION — the complete built-in directive universe
// ============================================================================

/// Declare the complete built-in directive universe ONCE. The macro
/// generates the closed typed kind (`DirectiveKind`), the complete
/// enumeration (`DirectiveKind::ALL`), and the descriptor table
/// (`DIRECTIVE_DESCRIPTORS`) from the same rows, so a directive cannot be
/// declared without acquiring its typed identity, its descriptor, and its
/// place in every enumeration-driven consumer — catalog publication and
/// realization dispatch included. A duplicate name is refused at compile
/// time by the uniqueness check below; a duplicate variant is refused by
/// the enum itself.
macro_rules! declare_directives {
    ($($(#[doc = $doc:expr])* $variant:ident = $name:literal {
        category: $category:expr,
        realization: $realization:expr,
        params: $params:expr,
        receipt_echoes: $echoes:expr,
        receipt_input_echo: $input_echo:expr,
        receipt_payload: $payload:expr,
        side_effects: $side_effects:expr $(,)?
    }),+ $(,)?) => {
        /// THE CLOSED DIRECTIVE KIND — one variant per declared built-in.
        /// Consumers match this exhaustively: adding a declaration breaks
        /// every match that must answer for it, at compile time.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum DirectiveKind { $($(#[doc = $doc])* $variant),+ }

        impl DirectiveKind {
            /// The complete enumeration, in declaration order — the one
            /// list tests and publication iterate.
            pub const ALL: &'static [DirectiveKind] = &[$(DirectiveKind::$variant),+];

            /// This kind's descriptor. Indexing is safe by construction:
            /// the enum and the table are generated from the same rows in
            /// the same order.
            pub const fn descriptor(self) -> &'static DirectiveDescriptor {
                &DIRECTIVE_DESCRIPTORS[self as usize]
            }
        }

        /// The declared directives' descriptors, one each, in declaration
        /// order — generated beside the kind, never edited apart from it.
        pub const DIRECTIVE_DESCRIPTORS: &[DirectiveDescriptor] = &[$(
            DirectiveDescriptor {
                name: $name,
                category: $category,
                namespace: STD_PRELUDE,
                realization: $realization,
                params: $params,
                receipt_echoes: $echoes,
                receipt_input_echo: $input_echo,
                receipt_payload: $payload,
                side_effects: $side_effects,
            }
        ),+];
    };
}

declare_directives! {
    // --- Session (15): direct the session's namespace tree; liminal-eligible.
    Consult = "consult" {
        category: DirectiveCategory::Session,
        realization: DirectiveRealization::Entity,
        params: &[p("file_path"), pn("namespace")],
        receipt_echoes: &[],
        receipt_input_echo: true,
        receipt_payload: ReceiptPayload::Namespaces,
        side_effects: true,
    },
    ConsultTree = "consult_tree" {
        category: DirectiveCategory::Session,
        realization: DirectiveRealization::Entity,
        params: &[p("dir_path"), pn("root_namespace")],
        receipt_echoes: &[e("path"), e("namespace")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::ConsultedFiles,
        side_effects: true,
    },
    Reconsult = "reconsult" {
        category: DirectiveCategory::Session,
        realization: DirectiveRealization::Entity,
        params: &[
            pn("namespace"),
            DirectiveParam {
                name: "new_file_path",
                kind: DirectiveParamKind::StringOrPath,
                optional: true,
            },
        ],
        receipt_echoes: &[e("namespace"), eo("path")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    Unconsult = "unconsult" {
        category: DirectiveCategory::Session,
        realization: DirectiveRealization::Entity,
        params: &[pn("namespace")],
        receipt_echoes: &[e("namespace")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    Mount = "mount" {
        category: DirectiveCategory::Session,
        realization: DirectiveRealization::Entity,
        params: &[p("db_path"), pn("namespace")],
        receipt_echoes: &[e("path"), e("namespace")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    MountNew = "mount_new" {
        category: DirectiveCategory::Session,
        realization: DirectiveRealization::Entity,
        params: &[p("db_path"), pn("namespace")],
        receipt_echoes: &[e("path"), e("namespace")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    MountTree = "mount_tree" {
        category: DirectiveCategory::Session,
        realization: DirectiveRealization::Entity,
        params: &[p("db_uri"), pn("namespace")],
        receipt_echoes: &[e("path"), e("namespace")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::Namespaces,
        side_effects: true,
    },
    Unmount = "unmount" {
        category: DirectiveCategory::Session,
        realization: DirectiveRealization::Entity,
        params: &[pn("namespace")],
        receipt_echoes: &[e("namespace")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    Refresh = "refresh" {
        category: DirectiveCategory::Session,
        realization: DirectiveRealization::Entity,
        params: &[pn("namespace")],
        receipt_echoes: &[e("namespace")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    Ground = "ground" {
        category: DirectiveCategory::Session,
        realization: DirectiveRealization::Entity,
        params: &[pn("data_ns"), pn("lib_ns"), pn("new_ns_name")],
        receipt_echoes: &[e("data_namespace"), e("lib_namespace"), e("namespace")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    Enlist = "enlist" {
        category: DirectiveCategory::Session,
        realization: DirectiveRealization::Entity,
        params: &[pn("namespace")],
        receipt_echoes: &[e("namespace"), eo("into")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    Delist = "delist" {
        category: DirectiveCategory::Session,
        realization: DirectiveRealization::Entity,
        params: &[pn("namespace")],
        receipt_echoes: &[e("namespace")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    Alias = "alias" {
        category: DirectiveCategory::Session,
        realization: DirectiveRealization::Entity,
        params: &[pn("namespace"), p("shorthand")],
        receipt_echoes: &[e("namespace"), e("shorthand")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    Expose = "expose" {
        category: DirectiveCategory::Session,
        realization: DirectiveRealization::LiminalOnly,
        params: &[pn("namespace")],
        receipt_echoes: &[],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    Doc = "doc" {
        category: DirectiveCategory::Session,
        realization: DirectiveRealization::Entity,
        params: &[p("target"), p("doc")],
        receipt_echoes: &[],
        receipt_input_echo: true,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    // --- DDL (5): create database objects. THE TARGET IS A PARAMETER: a
    // CREATION target is an ordinary argument, so it stands in the argument
    // group and the lone group of a one-group call is receipt access.
    TempTable = "temp_table" {
        category: DirectiveCategory::Ddl,
        realization: DirectiveRealization::SyntaxPipeTerminal,
        params: &[pt("target")],
        receipt_echoes: &[e("name")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    Table = "table" {
        category: DirectiveCategory::Ddl,
        realization: DirectiveRealization::SyntaxPipeTerminal,
        params: &[pt("target")],
        receipt_echoes: &[e("name")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    TempView = "temp_view" {
        category: DirectiveCategory::Ddl,
        realization: DirectiveRealization::SyntaxPipeTerminal,
        params: &[pt("target")],
        receipt_echoes: &[e("name")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    Imprint = "imprint" {
        category: DirectiveCategory::Ddl,
        realization: DirectiveRealization::Entity,
        params: &[p("source_ns"), p("target_ns")],
        receipt_echoes: &[e("source_namespace"), e("target_namespace")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::MaterializedEntities,
        side_effects: true,
    },
    ImprintReplace = "imprint_replace" {
        category: DirectiveCategory::Ddl,
        realization: DirectiveRealization::Entity,
        params: &[p("source_ns"), p("target_ns")],
        receipt_echoes: &[e("source_namespace"), e("target_namespace")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::MaterializedEntities,
        side_effects: true,
    },
    // --- DML (3): write rows in user tables.
    Insert = "insert" {
        category: DirectiveCategory::Dml(crate::names::DmlVerb::Insert),
        realization: DirectiveRealization::SyntaxPipeTerminal,
        params: &[pt("target")],
        receipt_echoes: &[e("target")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    Update = "update" {
        category: DirectiveCategory::Dml(crate::names::DmlVerb::Update),
        realization: DirectiveRealization::SyntaxPipeTerminal,
        params: &[pt("target")],
        receipt_echoes: &[e("target")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    Delete = "delete" {
        category: DirectiveCategory::Dml(crate::names::DmlVerb::Delete),
        realization: DirectiveRealization::SyntaxPipeTerminal,
        params: &[pt("target")],
        receipt_echoes: &[e("target")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    // --- Execution (2): start runs.
    Run = "run" {
        category: DirectiveCategory::Execution,
        realization: DirectiveRealization::Entity,
        params: &[p("file_path")],
        receipt_echoes: &[e("path")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::RunResult,
        side_effects: true,
    },
    RunNamespace = "run_namespace" {
        category: DirectiveCategory::Execution,
        realization: DirectiveRealization::Entity,
        params: &[p("namespace")],
        receipt_echoes: &[e("namespace")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::RunResult,
        side_effects: true,
    },
    // --- Utility: direct the run itself.
    Exit = "exit" {
        category: DirectiveCategory::Utility,
        realization: DirectiveRealization::SyntaxPipeTerminal,
        params: &[],
        receipt_echoes: &[],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    Abort = "abort" {
        category: DirectiveCategory::Utility,
        realization: DirectiveRealization::SyntaxPipeTerminal,
        params: &[p("identity"), po("label")],
        receipt_echoes: &[],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::None,
        side_effects: true,
    },
    Assert = "assert" {
        category: DirectiveCategory::Utility,
        realization: DirectiveRealization::SyntaxPipeTerminal,
        params: &[pr("property"), po("label")],
        receipt_echoes: &[eo("label")],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::Assertion,
        side_effects: true,
    },
    Returning = "returning" {
        category: DirectiveCategory::Utility,
        realization: DirectiveRealization::SyntaxPipeTerminal,
        params: &[],
        receipt_echoes: &[],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::Input,
        side_effects: false,
    },
    ReturningOther = "returning_other" {
        category: DirectiveCategory::Utility,
        realization: DirectiveRealization::SyntaxPipeTerminal,
        params: &[],
        receipt_echoes: &[],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::OtherRelation,
        side_effects: false,
    },
    Stdout = "stdout" {
        category: DirectiveCategory::Utility,
        realization: DirectiveRealization::SyntaxPipeTerminal,
        params: &[],
        receipt_echoes: &[],
        receipt_input_echo: false,
        receipt_payload: ReceiptPayload::Input,
        side_effects: true,
    },
}

// NO DUPLICATE NAME CAN ENTER THE DECLARATION: refused at compile time,
// beside the enum's own refusal of a duplicate variant.
const _: () = {
    const fn same(a: &str, b: &str) -> bool {
        let (a, b) = (a.as_bytes(), b.as_bytes());
        if a.len() != b.len() {
            return false;
        }
        let mut i = 0;
        while i < a.len() {
            if a[i] != b[i] {
                return false;
            }
            i += 1;
        }
        true
    }
    let mut i = 0;
    while i < DIRECTIVE_DESCRIPTORS.len() {
        let mut j = i + 1;
        while j < DIRECTIVE_DESCRIPTORS.len() {
            assert!(
                !same(DIRECTIVE_DESCRIPTORS[i].name, DIRECTIVE_DESCRIPTORS[j].name),
                "duplicate directive name in the declaration"
            );
            j += 1;
        }
        i += 1;
    }
};

impl DirectiveKind {
    /// The declared bare name, no `!` suffix.
    pub const fn bare_name(self) -> &'static str {
        self.descriptor().name
    }

    /// The identity as the language spells it: the bare name with the `!`.
    pub fn bang_name(self) -> String {
        format!("{}!", self.bare_name())
    }

    /// The declared kind a name (with or without the trailing `!`) means.
    /// `None` is not a fallthrough: it says the name is OUTSIDE the closed
    /// built-in universe — a user effect rule or an unknown identity, which
    /// resolution and refusal distinguish downstream.
    pub fn from_name(name: &str) -> Option<DirectiveKind> {
        let bare = name.strip_suffix('!').unwrap_or(name);
        DirectiveKind::ALL
            .iter()
            .copied()
            .find(|kind| kind.bare_name() == bare)
    }

    /// Select a built-in by its complete authored identity. Bare demands use
    /// the standard prelude's ambient visibility; a qualified demand must
    /// name the descriptor's declared namespace exactly. A different
    /// qualifier is not this built-in and proceeds to ordinary effect-rule
    /// resolution.
    pub fn select_identity(name: &str, namespace: Option<&str>) -> Option<DirectiveKind> {
        let kind = Self::from_name(name)?;
        match namespace {
            None => Some(kind),
            Some(namespace) if namespace == kind.descriptor().namespace => Some(kind),
            Some(_) => None,
        }
    }
}

/// Select a built-in directive from the full reference identity carried by
/// the AST. Consumers that possess a reference use this function rather than
/// discarding its qualifier and looking up only the local spelling.
pub fn kind_for_reference(
    reference: &crate::pipeline::asts::vocabulary::Ref,
) -> Option<DirectiveKind> {
    let namespace = reference.namespace_fq();
    DirectiveKind::select_identity(&reference.name_text(), namespace.as_deref())
}

/// Look up the descriptor selected by a complete AST reference identity.
pub fn descriptor_for_reference(
    reference: &crate::pipeline::asts::vocabulary::Ref,
) -> Option<&'static DirectiveDescriptor> {
    kind_for_reference(reference).map(DirectiveKind::descriptor)
}

/// Whether a complete authored reference belongs on the user effect-rule
/// road. An entity-backed standard directive with a wrong qualifier remains a
/// contextual entity miss so the entity resolver can name its true identity;
/// it is not selected as the built-in and cannot execute it. Syntax and
/// liminal names under another qualifier are ordinary qualified effect names.
pub fn is_user_effect_reference(reference: &crate::pipeline::asts::vocabulary::Ref) -> bool {
    if !reference.name_text().ends_with('!') || kind_for_reference(reference).is_some() {
        return false;
    }
    DirectiveKind::from_name(&reference.name_text())
        .is_none_or(|kind| kind.descriptor().realization != DirectiveRealization::Entity)
}

/// Look up the descriptor for a built-in directive name (with or without
/// the trailing `!`). `None` means the name is not a built-in — user
/// effect rules and unknown names alike.
pub fn descriptor(name: &str) -> Option<&'static DirectiveDescriptor> {
    DirectiveKind::from_name(name).map(DirectiveKind::descriptor)
}

/// Extract a directive's target designator from a preserved relational
/// argument: a whole-table access (`name(*)`), optionally
/// namespace-qualified. Anything else — filters, projections, anonymous
/// tables, derived expressions — refuses with a teaching diagnostic: a
/// target NAMES where the effect lands, it is not a relation to
/// evaluate. One interpreter for DDL and DML; the
/// badge and verb phrase say which family taught the refusal.
pub fn target_designator(
    bare: &str,
    badge: &'static str,
    verb_phrase: &str,
    argument: &Chain<Unresolved>,
) -> Result<(String, Option<String>)> {
    if let (
        Some(Relation::Ground {
            mention: GroundMention::Named { identifier, .. },
            ..
        }),
        Some(Access::All),
    ) = (argument.as_read_relation(), argument.head_access())
    {
        let ns = if identifier.namespace_path.is_empty() {
            None
        } else {
            Some(
                identifier
                    .namespace_path
                    .iter()
                    .map(|i| i.name.as_str())
                    .collect::<Vec<_>>()
                    .join("::"),
            )
        };
        return Ok((identifier.name.to_string(), ns));
    }
    Err(DelightQLError::validation_error_categorized(
        badge,
        format!(
            "{bare}!'s target is a whole-table DESIGNATOR — `name(*)`, optionally \
             namespace-qualified (`my::ns.name(*)`) — {verb_phrase}; \
             filters, projections, and derived relations do not belong in a \
             target"
        ),
        "target designator",
    ))
}

/// Classify a directive name (with or without the trailing `!`). Derived
/// from the authoritative descriptor table.
pub fn directive_category(name: &str) -> DirectiveCategory {
    descriptor(name)
        .map(|d| d.category)
        .unwrap_or(DirectiveCategory::User)
}

/// Is this directive name (with or without `!`) liminal-eligible?
/// Exactly the session directives are.
pub fn is_liminal_eligible(name: &str) -> bool {
    directive_category(name) == DirectiveCategory::Session
}

/// Badge for the liminal-eligibility refusal.
pub const LIMINAL_NOT_ELIGIBLE_BADGE: &str = "directive/liminal/not_eligible";

/// The liminal-eligibility refusal message. Substring pinned red-first by the
/// effects ball (liminal--41_dml_not_eligible, liminal--42_run_not_eligible:
/// "only session directives are liminal-eligible").
pub fn liminal_not_eligible_message(name: &str) -> String {
    let bare = name.strip_suffix('!').unwrap_or(name);
    format!(
        "cannot execute '{bare}!' in the liminal space: only session directives \
         are liminal-eligible — every other directive (DML, DDL, execution, \
         utility, and user effect rules) executes by demand, not at load \
         (EFFECT-ALGEBRA §8). Put it in an effect rule and demand it from main!."
    )
}

// ============================================================================
// Directive invocations (the normalized records validators walk)
// ============================================================================

/// A single directive invocation found in an expression: name (with `!`),
/// category, and scalar parameters. Relational parameters remain on the
/// common `FunctorCall` and are not reclassified here.
#[derive(Debug, Clone)]
pub struct DirectiveInvocation {
    /// Directive name, `!` included (e.g. `"insert!"`, `"route!"`).
    pub name: String,
    /// The category of the name (`User` for effect-rule names).
    pub category: DirectiveCategory,
    /// The invocation's scalar parameters, as written.
    /// Consumed by the effect transformer; the validators read only
    /// `name`/`category`.
    #[allow(dead_code)]
    pub params: Vec<DomainExpression<Unresolved>>,
}

// ============================================================================
// Liminal directives
// ============================================================================

/// A directive statement in a file's liminal space — the top level of a
/// file, outside the rules. Only session directives are liminal-eligible.
#[derive(Debug, Clone)]
pub struct LiminalDirective {
    /// Directive name WITHOUT the `!` (matches the extraction layer's
    /// `EmbeddedDirective` convention).
    pub name: String,
    /// Naive string arguments, quotes stripped.
    pub args: Vec<String>,
}

// ============================================================================
// Effect rules (R1–R9)
// ============================================================================

/// One clause (arm) of an effect rule (R5: clauses are arms).
#[derive(Debug, Clone)]
pub struct EffectClause {
    /// The clause's head — the same `Head` every other neck carries. An
    /// effect rule is a `!`-marked SUBJECT, not a parallel head family.
    pub head: Head,
    /// The clause body, with effect-CTE definitions separated out.
    pub body: EffectBody,
}

/// An effect rule: a user directive definition, possibly multi-clause (R5).
#[derive(Debug, Clone)]
pub struct EffectRule {
    /// Rule name, `!` included (e.g. `"route!"`, `"main!"`).
    pub name: String,
    pub clauses: Vec<EffectClause>,
}

/// Group a CTE list by subject, assemble each group's heads once, and spend
/// them (S10's grouping, S04's law). Clause order is preserved: a clause
/// comes back where it was written, because that is the order its arms
/// accumulate in.
fn assemble_cte_subjects(ctes: Vec<CteBinding<Unresolved>>) -> Result<Vec<CteBinding<Unresolved>>> {
    use crate::pipeline::asts::core::definitions::{assemble, spend_heads, Head};
    use delightql_types::SqlIdentifier;

    let mut order: Vec<SqlIdentifier> = Vec::new();
    let mut groups: HashMap<SqlIdentifier, Vec<(usize, CteBinding<Unresolved>)>> = HashMap::new();
    for (position, cte) in ctes.into_iter().enumerate() {
        // An effect body's bindings are authored: a generated or structural
        // subject is a compiler product, and none exists in an authored body.
        let Some(name) = cte.subject().authored_name().cloned() else {
            return Err(DelightQLError::transformation_error(
                "an effect body's binding carries no authored subject; compiler-built \
                 bindings cannot stand in an authored body",
                "effect_cte",
            ));
        };
        if !groups.contains_key(&name) {
            order.push(name.clone());
        }
        groups.entry(name).or_default().push((position, cte));
    }

    let mut placed: Vec<(usize, CteBinding<Unresolved>)> = Vec::new();
    for name in order {
        let group = groups
            .remove(&name)
            .expect("every ordered name has a group");
        let (positions, bindings): (Vec<usize>, Vec<CteBinding<Unresolved>>) =
            group.into_iter().unzip();
        let heads: Vec<&Head> = bindings.iter().map(|cte| &cte.authority().head).collect();
        let assembly = assemble(
            name.as_str(),
            &heads,
            crate::pipeline::asts::core::definitions::GroundNaming::Refuse,
        )?;
        placed.extend(
            positions
                .into_iter()
                .zip(spend_heads(bindings, &assembly, name.as_str())?),
        );
    }
    placed.sort_by_key(|(position, _)| *position);
    Ok(placed.into_iter().map(|(_, cte)| cte).collect())
}

/// An effect-clause body (R3): the clause's query-local block + the body
/// expression whose value is the clause's value.
#[derive(Debug, Clone)]
pub struct EffectBody {
    /// THE CLAUSE'S OWN BLOCK, moved here whole. Pure bindings enter the
    /// ordinary CTE resolver with their heads, provenance and fixpoint
    /// claim intact; effect-marked bindings are opened only by their demand
    /// judgment — and the one name fact that decides which is which travels
    /// with them, never beside them.
    pub locals: crate::pipeline::asts::core::QueryLocals<Unresolved>,
    pub expression: Chain<Unresolved>,
}

impl EffectBody {
    /// The relation bindings this body declares.
    pub fn ctes(&self) -> &[CteBinding<Unresolved>] {
        self.locals.ctes()
    }

    /// Build an `EffectBody` view over a parsed body `Query`, reading each
    /// CTE binding's effect marker. Dropping the marker here would make an
    /// effectful binding read as pure; pinned by
    /// `effect_cte_marker_is_read_by_builder`.
    pub fn from_query(query: &Query<Unresolved>) -> Result<EffectBody> {
        let mut locals = query.locals.clone();
        // The subject's clauses meet at the ONE assembler and each body
        // then carries the projection its own head declares — the same
        // authority and the same law the pure road runs, because a head
        // means one thing wherever it is written. An effect CTE reaches its
        // demand site with its head already spent, so the mention
        // instantiates the CONTRACT, not the body the contract narrows.
        locals.restate_ctes(assemble_cte_subjects)?;
        Ok(EffectBody {
            locals,
            expression: query.body.clone(),
        })
    }
}

impl EffectRule {
    /// Assemble an `EffectRule` from one assembled definition group.
    ///
    /// The group is already one subject of one declared kind — the
    /// assembler decided that, for every definition form, before the rule
    /// was ever registered. There is no second kind-agreement check here to
    /// disagree with it.
    /// Shape one OPENED definition group into the typed rule. The
    /// declaration environment is not a field here: the invocation road
    /// (the definition-use authority) admits the family and applies its
    /// environment; a rule value carries no namespace string to re-pair.
    pub fn from_definition_group(group: &DefinitionGroup) -> Result<EffectRule> {
        let name = group.name();
        let name = name.as_str();
        let mut clauses = Vec::new();
        for def in group.clauses() {
            let DdlBody::Relational(ref query) = def.body else {
                return Err(DelightQLError::validation_error_categorized(
                    "effect/rule/body_grammar",
                    format!(
                        "effect rule '{}': body is not a relational expression \
                         (EFFECT-ALGEBRA R3)",
                        name
                    ),
                    "non-relational effect body",
                ));
            };
            clauses.push(EffectClause {
                head: def.head.clone(),
                body: EffectBody::from_query(query)?,
            });
        }
        Ok(EffectRule {
            name: name.to_string(),
            clauses,
        })
    }

    /// Drop the declared scalar and rule-valued formals from every clause
    /// HEAD: the bound parameters leave the head, so landing decisions
    /// read only what the rule still wants. The BODIES are untouched —
    /// their parameter references bind through the invocation's formal
    /// frame, never by substituting caller text.
    pub fn strip_bound_param_heads(&mut self) {
        for clause in &mut self.clauses {
            if let Some(params) = clause.head.ho_params.as_mut() {
                params.retain(|param| {
                    !matches!(param, HoParam::Scalar { .. } | HoParam::Rule { .. })
                });
            }
        }
    }
}

// ============================================================================
// The demand walker
// ============================================================================

/// Collect every directive invocation in an expression, in syntactic order.
///
/// Rides the shared whole-tree closure `AstVisit`, so it reaches EVERY
/// query-bearing edge — `Filter.condition`, `correlation`, pipe-operator
/// argument subqueries, HO table arguments. A private walker matching only
/// `Filter { source, .. }` leaves a directive hidden under an IN/EXISTS/scalar
/// predicate invisible to every rule that reads this collector.
pub fn collect_directive_invocations(expr: &Chain<Unresolved>) -> Vec<DirectiveInvocation> {
    let mut c = DirectiveDemandCollector::default();
    // The collector's hooks never fail, so the walk is infallible.
    let _ = walk_visit_relational(&mut c, expr);
    c.out
}

/// Collect every directive invocation in a full body query (CTEs included).
pub fn collect_directive_invocations_in_query(
    query: &Query<Unresolved>,
) -> Vec<DirectiveInvocation> {
    let mut c = DirectiveDemandCollector::default();
    let _ = walk_visit_query(&mut c, query);
    c.out
}

/// Does this expression demand a directive, directly or through a nested
/// subquery? (R1's and R4's criterion. Demands through CTE LABELS are seen
/// at the label's reference site — a reference to an effect CTE is written
/// `label!(*)`, which walks as a `!`-named call.)
pub fn expression_demands_directive(expr: &Chain<Unresolved>) -> bool {
    !collect_directive_invocations(expr).is_empty()
}

/// R2: does the body expression END in a directive? The "end" is the
/// rightmost step of the expression: the last pipe operator, the rightmost
/// conjunct of a join, or every arm of a union. Witness postfixes (`+`,
/// `\+`, `+-`) pass through — the algebra's own ledger tail applies them to
/// receipt arms. Pinned red-first by the effects
/// ball (rules--26_r2_ending).
#[stacksafe::stacksafe]
pub fn ends_in_directive(expr: &Chain<Unresolved>) -> bool {
    // Rides the chain's own tail fold: a union ends in a directive iff EVERY
    // arm does (`!empty && all`, the ledger shape). The per-node ending test
    // is `ends_in_directive_leaf`; only the member/bag recursion is shared.
    expr.fold_tail(&ends_in_directive_leaf, &|arms: Vec<bool>| {
        !arms.is_empty() && arms.iter().all(|b| *b)
    })
}

/// The tail-LEAF half of `ends_in_directive`: does THIS tail node (a Pipe's tail
/// operator, or a leaf relation) end in a directive? Witness totalizers keep the
/// underlying arm's ending (re-rooting the tail fold at `pipe.source`); a
/// trailing Filter / ER chain does not end in a directive.
fn ends_in_directive_leaf(expr: &Chain<Unresolved>) -> bool {
    let is_directive = |call: &crate::pipeline::asts::core::SealedCall<Unresolved>| {
        call.call().callee.name_text().ends_with('!')
    };
    let Some((last, prefix)) = expr.split_last() else {
        // A bare head ends in a directive exactly when it IS one.
        return match expr.head().form() {
            crate::pipeline::asts::core::GroundForm::Reference(Relation::FunctorCall {
                call,
                ..
            }) => is_directive(call),
            _ => false,
        };
    };
    match last.form() {
        // An access that singles out NO dimensions is the whole operand, so a
        // directive under one still ends the chain. One that reshapes a
        // heading does not: the chain then ends in the reshaping.
        crate::pipeline::asts::core::Continuation::Access { access, .. } if access.is_whole() => {
            ends_in_directive(&prefix.to_chain())
        }
        // A witness totalizer keeps the underlying arm's ending.
        crate::pipeline::asts::core::Continuation::Structural(
            crate::pipeline::asts::core::StructuralStep {
                form:
                    crate::pipeline::asts::core::StructuralForm::Witness { .. }
                    | crate::pipeline::asts::core::StructuralForm::SignedWitness,
                ..
            },
        ) => ends_in_directive(&prefix.to_chain()),
        // Operator-KIND classification: a tail pipe operator is never a
        // directive terminal — a directive call is a relation-position call
        // heading its chain — so the chain does not end in a directive,
        // regardless of subqueries in the operator's own argument domain
        // expressions, which the tail contract DELIBERATELY does not recurse
        // (descending would be the over-recursion bug).
        crate::pipeline::asts::core::Continuation::Pipe { .. } => false,
        // A trailing restriction or ER edge does NOT end in a directive; their
        // recursive fields are deliberately not descended (the tail contract).
        // Members and bag operations never reach the leaf — the tail fold
        // recurses them.
        _ => false,
    }
}

/// The names (with `!`) of all directives a clause body demands, EXCLUDING
/// references to the body's own effect-CTE labels (a reference `n!(*)` to a
/// CTE labeled `: n!` demands the CTE, not a rule named `n!` — E2). Used by
/// the R6 recursion check and the R9 positional checks.
pub fn demanded_directive_names(body: &EffectBody) -> Vec<DirectiveInvocation> {
    let mut c = DirectiveDemandCollector::default();
    for cte in body.ctes() {
        let _ = walk_visit_relational(&mut c, cte.body());
    }
    let _ = walk_visit_relational(&mut c, &body.expression);
    let labels: Vec<String> = body
        .ctes()
        .iter()
        .filter_map(|cte| cte.subject().authored_name())
        .map(|name| format!("{}!", name))
        .collect();
    c.out.retain(|inv| !labels.contains(&inv.name));
    c.out
}

/// Does the truth expression `b` demand a directive anywhere in its
/// boolean/domain subtree (through IN/EXISTS/scalar subqueries)? Used by the
/// effect transformer's lowering walker (W4) to detect an effect-head
/// predicate directive — legal in principle, but not yet lowerable (Q-I1(b)).
pub fn boolean_demands_directive(b: &TruthExpression<Unresolved>) -> bool {
    let mut c = DirectiveDemandCollector::default();
    let _ = walk_visit_boolean(&mut c, b);
    !c.out.is_empty()
}

/// Does the domain expression `d` demand a directive anywhere in its subtree?
pub fn domain_demands_directive(d: &DomainExpression<Unresolved>) -> bool {
    let mut c = DirectiveDemandCollector::default();
    let _ = walk_visit_domain(&mut c, d);
    !c.out.is_empty()
}

/// Does the pipe operator `op` demand a directive inside one of its argument
/// domain expressions (a scalar subquery hidden in a Transform/MapCover/…)?
/// The directive-bearing operators themselves (DML / directive terminals) are
/// lowered on the spine; this catches directives smuggled into a *pure*
/// operator's arguments.
pub fn operator_demands_directive(op: &PipeOp<Unresolved>) -> bool {
    let mut c = DirectiveDemandCollector::default();
    let _ = walk_visit_operator(&mut c, op);
    !c.out.is_empty()
}

/// Does this access/access demand a directive (a scalar subquery hidden in
/// a positional column expression)? Used by the lowering walker (W4) to close
/// the recursive type: a directive smuggled into a Ground read's access spec or
/// a DML terminal's access spec is OFF the lowered spine, so it must be refused
/// rather than passed to SQL unprocessed.
pub fn access_demands_directive(spec: &Access<Unresolved>) -> bool {
    let mut c = DirectiveDemandCollector::default();
    let _ = walk_visit_access(&mut c, spec);
    !c.out.is_empty()
}

/// The `AstVisit` tenant that realizes the whole-tree directive-demand closure.
/// The default `AstVisit` walk performs
/// the complete structural descent; this collector only names the demand
/// positions. Demand ORDER (load-bearing for R9's positional reads and for the
/// lowering walker): EVERY directive is recorded
/// on `exit_*`, so a directive nested in another's argument is demanded first
/// (inputs before invocation). Pinned by the effects ball's rules--79/80/81 (a directive
/// under a PURE head, now seen through a predicate subquery, so R1 refuses) and
/// by `nested_directive_argument_is_demanded_before_enclosing` (the order).
#[derive(Default)]
struct DirectiveDemandCollector {
    out: Vec<DirectiveInvocation>,
}

impl AstVisit<Unresolved> for DirectiveDemandCollector {
    // INPUTS BEFORE INVOCATION: EVERY directive form is
    // recorded on `exit_*`, AFTER its argument expressions have been descended.
    // A directive is thus demanded AFTER the demands nested in its arguments
    // (inner-before-outer), CONSISTENT across all forms — arguments are inputs,
    // so their demands precede the enclosing invocation. Recording some forms
    // on `enter_` while others record on exit misbinds the order
    // (outer-before-inner for the enter-recorded forms).
    // Pinned by `nested_directive_argument_is_demanded_before_enclosing`.
    fn exit_relation(&mut self, r: &Relation<Unresolved>) -> Result<Descent> {
        if let Relation::FunctorCall { call, .. } = r {
            let Some(reference) = Some(&call.call().callee) else {
                return Ok(Descent::Continue);
            };
            let name = reference.name_text();
            if !name.ends_with('!') {
                return Ok(Descent::Continue);
            }
            self.out.push(DirectiveInvocation {
                name: name.to_string(),
                category: kind_for_reference(reference)
                    .map(|kind| kind.descriptor().category)
                    .unwrap_or(DirectiveCategory::User),
                params: call.call().arguments.value_domains().cloned().collect(),
            });
        }
        Ok(Descent::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// THE KIND AND THE TABLE ARE ONE DECLARATION: the enumeration, the
    /// descriptors, and both name directions round-trip — so a directive
    /// added to the declaration is IN the complete enumeration that
    /// publication and realization iterate, with nothing to update beside
    /// the declaration itself.
    #[test]
    fn the_kind_and_the_table_are_one_declaration() {
        assert_eq!(DirectiveKind::ALL.len(), DIRECTIVE_DESCRIPTORS.len());
        for kind in DirectiveKind::ALL {
            assert_eq!(DirectiveKind::from_name(&kind.bang_name()), Some(*kind));
            assert_eq!(DirectiveKind::from_name(kind.bare_name()), Some(*kind));
            assert_eq!(
                DirectiveKind::select_identity(kind.bare_name(), None),
                Some(*kind)
            );
            assert_eq!(
                DirectiveKind::select_identity(
                    &kind.bang_name(),
                    Some(kind.descriptor().namespace)
                ),
                Some(*kind)
            );
            assert_eq!(
                DirectiveKind::select_identity(kind.bare_name(), Some("bogus")),
                None
            );
        }
    }

    /// A name outside the closed universe is OUTSIDE — a user rule or an
    /// unknown identity — and stays refusable downstream; reflection of
    /// the built-ins adds no fallthrough that mistakes it for one.
    #[test]
    fn a_name_outside_the_universe_is_outside() {
        assert_eq!(DirectiveKind::from_name("frobnicate!"), None);
        assert_eq!(DirectiveKind::from_name("route!"), None);
        assert_eq!(directive_category("frobnicate!"), DirectiveCategory::User);
    }

    /// The category taxonomy is the single source of truth for liminal
    /// eligibility and the R9 positional checks — pin the boundaries.
    #[test]
    fn directive_categories_match_effect_algebra_section_3() {
        assert_eq!(directive_category("consult!"), DirectiveCategory::Session);
        assert_eq!(directive_category("doc"), DirectiveCategory::Session);
        assert_eq!(directive_category("temp_table!"), DirectiveCategory::Ddl);
        assert_eq!(
            directive_category("insert!"),
            DirectiveCategory::Dml(crate::names::DmlVerb::Insert)
        );
        assert_eq!(directive_category("run!"), DirectiveCategory::Execution);
        assert_eq!(
            directive_category("run_namespace!"),
            DirectiveCategory::Execution
        );
        assert_eq!(directive_category("returning!"), DirectiveCategory::Utility);
        assert_eq!(directive_category("route!"), DirectiveCategory::User);

        // Exactly the session directives are liminal-eligible.
        assert!(is_liminal_eligible("mount!"));
        // mount_new! is a session directive,
        // liminal-eligible like mount!.
        assert_eq!(directive_category("mount_new!"), DirectiveCategory::Session);
        assert!(is_liminal_eligible("mount_new!"));
        assert!(!is_liminal_eligible("insert!"));
        assert!(!is_liminal_eligible("run!"));
        assert!(!is_liminal_eligible("route!"));
    }

    #[test]
    fn descriptors_judge_target_roles_and_receipt_arity_together() {
        let insert = descriptor("insert!").expect("insert is declared");
        assert_eq!(insert.params.len(), 1);
        assert_eq!(insert.params[0].kind, DirectiveParamKind::RelationTarget);

        let exact = Access::from_terms(
            ["success", "operation", "target"]
                .into_iter()
                .map(|name| DomainExpression::lvar_builder(name).build())
                .collect(),
        );
        insert
            .judge_invocation(1, true, &exact)
            .expect("one target and the exact receipt shape are admitted");

        let short = Access::from_terms(vec![DomainExpression::lvar_builder("only").build()]);
        let error = insert
            .judge_invocation(1, true, &short)
            .expect_err("a short receipt group refuses");
        assert_eq!(
            error.error_uri(),
            "delightql-error://semantic/directive/invocation/access"
        );
    }

    // ------------------------------------------------------------------------
    // Whole-tree directive-demand closure
    //
    // A walker matching `Filter { source, .. }` and dropping `condition`
    // leaves a directive hidden under an IN/EXISTS/scalar predicate invisible
    // to R1/R4/R6/R9 — all of which read this collector. These pins
    // prove the migrated `AstVisit` closure reaches those positions. R1 is
    // additionally pinned end-to-end by the effects ball's
    // rules--79/80/81_r1_predicate_{in,exists,scalar}.
    // ------------------------------------------------------------------------

    use crate::pipeline::asts::core::expressions::metadata_types::FilterOrigin;
    use crate::pipeline::asts::core::QualifiedName;

    fn qn(name: &str) -> QualifiedName {
        QualifiedName {
            namespace_path: crate::pipeline::asts::core::metadata::NamespacePath::empty(),
            name: name.into(),
        }
    }

    /// A directive demand sentinel: `route!(*)` as an expression-position call.
    fn directive(name: &str) -> Chain<Unresolved> {
        Chain::authored(GroundForm::Reference(Relation::FunctorCall {
            alias: None,
            call: crate::pipeline::asts::core::FunctorCall::written(
                crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
                    &std::rc::Rc::new(crate::names::Registry::new(&[])),
                    crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
                    name,
                ),
                vec![],
            )
            .into(),
        }))
    }

    /// A non-directive relation (a bare Ground read) — the collector records
    /// nothing for it, so it is inert scaffolding around the demand sentinels.
    fn plain() -> Chain<Unresolved> {
        Chain::read(
            Relation::Ground {
                mention: GroundMention::Named {
                    identifier: qn("rows"),
                    alias: None,
                    mutation_target: false,
                    passthrough: false,
                },
                outer: false,
            },
            crate::pipeline::asts::core::Access::All,
        )
    }

    fn filter_with_predicate(pred: TruthExpression<Unresolved>) -> Chain<Unresolved> {
        plain().then(Step::authored(Continuation::Restrict {
            condition: pred,
            origin: FilterOrigin::UserWritten,
        }))
    }

    fn in_relational(sub: Chain<Unresolved>) -> TruthExpression<Unresolved> {
        TruthExpression::RelationalMembership(RelationalMembership {
            probe: Probe::Value(Box::new(DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Open(
                    crate::pipeline::asts::core::DomainHole::Disregarded,
                ),
            ))),
            relation: Box::new(sub),
            addressing: ProbeAddressing {
                identifier: qn("p"),
                using_columns: vec![],
            },
            negated: false,
        })
    }

    /// A truth whose content is beside the point: the walk under test is
    /// looking for a directive, and this is a term that holds none.
    fn plain_comparison() -> TruthExpression<Unresolved> {
        TruthExpression::Comparison(Comparison {
            operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
            left: Box::new(DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Ground(
                    crate::pipeline::asts::core::LiteralValue::Number("1".into()),
                ),
            )),
            right: Box::new(DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Ground(
                    crate::pipeline::asts::core::LiteralValue::Number("1".into()),
                ),
            )),
        })
    }

    fn inner_exists(sub: Chain<Unresolved>) -> TruthExpression<Unresolved> {
        TruthExpression::Existence(Existence {
            polarity: Polarity::Positive,
            relation: Box::new(sub),
            addressing: ProbeAddressing {
                identifier: qn("p"),
                using_columns: vec![],
            },
        })
    }

    fn scalar_cmp(sub: Chain<Unresolved>) -> TruthExpression<Unresolved> {
        TruthExpression::Comparison(Comparison {
            operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
            left: Box::new(DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Scalarized(
                    crate::pipeline::asts::core::ScalarRelation::Named {
                        identifier: qn("s"),
                        body: Box::new(crate::pipeline::asts::core::ScalarizedRelation::authored(
                            sub,
                            crate::pipeline::asts::core::Scalarization::BoundToOne {
                                ordering: Vec::new(),
                            },
                        )),
                    },
                ),
            )),
            right: Box::new(DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Open(
                    crate::pipeline::asts::core::DomainHole::Disregarded,
                ),
            )),
        })
    }

    #[test]
    fn demand_reaches_predicate_subqueries_in_exists_scalar() {
        for build in [
            in_relational as fn(Chain<Unresolved>) -> TruthExpression<Unresolved>,
            inner_exists,
            scalar_cmp,
        ] {
            let expr = filter_with_predicate(build(directive("route!")));
            let found = collect_directive_invocations(&expr);
            assert_eq!(
                found.iter().map(|i| i.name.as_str()).collect::<Vec<_>>(),
                vec!["route!"],
                "directive under a predicate subquery must be a visible demand"
            );
            assert!(expression_demands_directive(&expr));
            assert!(boolean_demands_directive(&build(directive("route!"))));
        }
    }

    #[test]
    fn nested_directive_argument_is_demanded_before_enclosing() {
        // INPUTS BEFORE INVOCATION: a directive nested in
        // another directive's ARGUMENT is demanded FIRST (inner-before-outer),
        // because every directive is recorded on `exit_*` — after its arguments
        // are descended. Recording expression-position calls on `enter_`
        // misbinds: outer-before-inner.
        let inner_in_arg = DomainExpression::Application(
            crate::pipeline::asts::core::FunctionApplication::Scalarized(
                crate::pipeline::asts::core::ScalarRelation::Named {
                    identifier: qn("s"),
                    body: Box::new(crate::pipeline::asts::core::ScalarizedRelation::authored(
                        directive("inner!"),
                        crate::pipeline::asts::core::Scalarization::BoundToOne {
                            ordering: Vec::new(),
                        },
                    )),
                },
            ),
        );
        let outer = Chain::authored(GroundForm::Reference(Relation::FunctorCall {
            alias: None,
            call: crate::pipeline::asts::core::FunctorCall::written(
                crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
                    &std::rc::Rc::new(crate::names::Registry::new(&[])),
                    crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
                    "outer!",
                ),
                vec![crate::pipeline::asts::core::operators::HoArgument::Value(
                    crate::pipeline::asts::core::ArgumentValue::plain(inner_in_arg),
                )],
            )
            .into(),
        }));
        let invs = collect_directive_invocations(&outer);
        let order: Vec<&str> = invs.iter().map(|i| i.name.as_str()).collect();
        assert_eq!(
            order,
            vec!["inner!", "outer!"],
            "an argument's demands precede the enclosing invocation"
        );
    }

    #[test]
    fn access_demands_directive_reaches_positional_scalar_subquery() {
        use crate::pipeline::asts::core::Access;
        // A directive hidden in a scalar subquery in a positional access column
        // (a Ground read's or DML terminal's access spec). The builder currently
        // routes non-column access expressions to WHERE filters, so this shape
        // is not reachable via surface DQL today — but the closure reaches it, so
        // the lowering walker (W4) refuses it as defense-in-depth against any
        // future construction path.
        let spec = Access::from_terms(vec![DomainExpression::Application(
            crate::pipeline::asts::core::FunctionApplication::Scalarized(
                crate::pipeline::asts::core::ScalarRelation::Named {
                    identifier: qn("s"),
                    body: Box::new(crate::pipeline::asts::core::ScalarizedRelation::authored(
                        directive("insert!"),
                        crate::pipeline::asts::core::Scalarization::BoundToOne {
                            ordering: Vec::new(),
                        },
                    )),
                },
            ),
        )]);
        assert!(access_demands_directive(&spec));
        assert!(!access_demands_directive(&Access::All));
    }

    #[test]
    fn demand_reaches_deeply_nested_boolean_composition() {
        // NOT( plain-ish AND (plain OR EXISTS(route!)) ) — the demand sits
        // under three layers of boolean composition, so only genuine recursion
        // finds it.
        let deep = TruthExpression::Not {
            expr: Box::new(
                TruthExpression::all(vec![
                    plain_comparison(),
                    TruthExpression::any(vec![
                        plain_comparison(),
                        inner_exists(directive("route!")),
                    ])
                    .expect("two terms"),
                ])
                .expect("two terms"),
            ),
        };
        let expr = filter_with_predicate(deep);
        let found = collect_directive_invocations(&expr);
        assert_eq!(found.len(), 1, "deeply nested demand must be reached");
        assert_eq!(found[0].name, "route!");
    }

    #[test]
    fn demand_reaches_correlation_and_operator_arguments() {
        // Join condition (via InnerExists) — missed by the old walker.
        let join = plain().then(Step::authored(Continuation::Member {
            rhs: plain(),
            correlation: Some(
                crate::pipeline::ast_unresolved::MemberCorrelation::Condition(inner_exists(
                    directive("route!"),
                )),
            ),
            join_type: None,
        }));
        assert!(
            boolean_demands_directive(&inner_exists(directive("route!"))),
            "join-condition helper must see the nested demand"
        );
        assert_eq!(collect_directive_invocations(&join).len(), 1);

        // Pipe-OPERATOR argument (a scalar subquery inside a Transform) — the
        // edge no relational-entry walker reached before.
        let op = PipeOp::Transform {
            items: crate::pipeline::asts::vocabulary::Vec1::new(
                crate::pipeline::asts::core::NamedOutItem::authored(
                    DomainExpression::Application(
                        crate::pipeline::asts::core::FunctionApplication::Scalarized(
                            crate::pipeline::asts::core::ScalarRelation::Named {
                                identifier: qn("s"),
                                body: Box::new(
                                    crate::pipeline::asts::core::ScalarizedRelation::authored(
                                        directive("route!"),
                                        crate::pipeline::asts::core::Scalarization::BoundToOne {
                                            ordering: Vec::new(),
                                        },
                                    ),
                                ),
                            },
                        ),
                    ),
                    "a".into(),
                    None,
                ),
            ),
            guard: None,
        };
        assert!(operator_demands_directive(&op));
        let pipe = plain().then(Step::authored(Continuation::Pipe {
            operator: op,
            named: None,
        }));
        assert_eq!(collect_directive_invocations(&pipe).len(), 1);
    }
}
