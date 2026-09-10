// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `semantic/directive/…` — invoking directives.

use super::Semantic;
use crate::diagnostic::{DelightQLError, Taxon};

/// `semantic/directive/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Directive))]
pub enum Directive {
    /// How a directive's authored arguments bind to its declared parameter
    /// row: their count and their values.
    #[family(
        "binding",
        summary = "A directive's arguments do not bind to its parameters."
    )]
    #[error(transparent)]
    Binding(DirectiveBinding),

    /// A receipt was read through an access shape the directive's receipt
    /// heading does not admit.
    #[leaf("chain/receipt_shape", class = Syntax, summary = "A receipt was read with the wrong shape.")]
    #[error("Validation error: {message}")]
    ChainReceiptShape { message: String },

    /// consult! into a namespace that already exists refuses: a load has
    /// one destination, and an existing namespace is either reconsulted
    /// (reconsult!) or left alone.
    #[leaf("consult/exists", class = Syntax, summary = "consult! targets an existing namespace.")]
    #[error("Validation error: {message}")]
    ConsultExists { message: String },

    /// Where a directive may stand: some only in a liminal program, some
    /// only as a pipe terminal.
    #[family(
        "context",
        summary = "A directive stood in a context it is not admitted in."
    )]
    #[error(transparent)]
    Context(DirectiveContext),

    /// A directive was invoked with an access group it does not publish:
    /// exact positional receipt access is judged before arity, so a
    /// one-group call that names no receipt column is taught here.
    #[leaf("invocation/access", class = Syntax, summary = "A directive was accessed with a shape it does not publish.")]
    #[error("Validation error: {message}")]
    InvocationAccess { message: String },

    /// The directive's receipt carries no payload, so a payload read
    /// (`!>`) has nothing to release.
    #[leaf("receipt/no_payload", class = Syntax, summary = "A receipt without a payload was released.")]
    #[error("Validation error: {message}")]
    ReceiptNoPayload { message: String },

    /// The name is not a directive the closed built-in population
    /// declares.
    #[leaf("unknown", class = Syntax, summary = "An unknown directive.")]
    #[error("Validation error: {message}")]
    Unknown { message: String },

    /// Only session directives are liminal-eligible: a liminal program
    /// (consult!, reconsult!, consult_tree!) executes session directives
    /// and relational goals, and nothing else.
    #[leaf("liminal/not_eligible", class = Syntax, summary = "A statement is not liminal-eligible.")]
    #[error("Validation error: {message}")]
    LiminalNotEligible { message: String },

    /// unconsult! could not compensate an external effect the load had
    /// made, so the namespace cannot be unloaded cleanly.
    #[leaf("unconsult/uncompensable", class = Syntax, summary = "unconsult! cannot compensate the load's effects.")]
    #[error("Validation error: {message}")]
    UnconsultUncompensable { message: String },

    /// unmount! inside a consulted program would rearrange a namespace that
    /// predates the program; a file describes its library rather than its
    /// caller's session, so the program refuses.
    #[leaf("unmount/uncompensable", class = Syntax, summary = "unmount! of a pre-existing namespace inside a consulted program.")]
    #[error("Validation error: {message}")]
    UnmountUncompensable { message: String },

    /// reconsult! inside a consulted program would reload a namespace that
    /// predates the program; the program refuses rather than leave the
    /// session half-changed if it is torn down.
    #[leaf("reconsult/uncompensable", class = Syntax, summary = "reconsult! of a pre-existing namespace inside a consulted program.")]
    #[error("Validation error: {message}")]
    ReconsultUncompensable { message: String },

    /// consult_concat! could not compensate an external effect an earlier
    /// segment had made.
    #[leaf("consult_concat/uncompensable", class = Syntax, summary = "consult_concat! cannot compensate an earlier segment's effects.")]
    #[error("Validation error: {message}")]
    ConsultConcatUncompensable { message: String },
}

/// `semantic/directive/binding/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Directive, Directive::Binding))]
pub enum DirectiveBinding {
    /// The directive received a different number of arguments than its
    /// declared parameter row takes. The message states both.
    #[leaf("arity", class = Syntax, summary = "A directive received the wrong number of arguments.")]
    #[error("Validation error: {message}")]
    Arity { message: String },

    /// A directive parameter is a value — a namespace, a path, a label — and
    /// the argument written is not one: a table, a truth, an expression the
    /// directive cannot read.
    #[leaf("value", class = Syntax, summary = "A directive argument is not a value it can read.")]
    #[error("Validation error: {message}")]
    Value { message: String },
}

/// `semantic/directive/context/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Directive, Directive::Context))]
pub enum DirectiveContext {
    /// This directive is admitted only inside a liminal program: it acts on
    /// the load in progress and has no meaning at a prompt.
    #[leaf("liminal_only", class = Syntax, summary = "A liminal-only directive stood outside a load.")]
    #[error("Validation error: {message}")]
    LiminalOnly { message: String },

    /// This directive is a pipe terminal: it takes its input from the pipe
    /// and stands nowhere else.
    #[leaf("pipe_terminal", class = Syntax, summary = "A pipe-terminal directive stood outside a pipe.")]
    #[error("Validation error: {message}")]
    PipeTerminal { message: String },
}
