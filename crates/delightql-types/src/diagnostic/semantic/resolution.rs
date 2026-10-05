// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `semantic/resolution/…` — name binding.

use super::Semantic;
use crate::diagnostic::{DelightQLError, Taxon};

/// The resolution family. Its own path is an emitted identity: a
/// name-binding failure no member describes more precisely.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Resolution))]
pub enum Resolution {
    /// A name-binding failure no member describes more precisely: the
    /// message names what was looked for and where.
    #[leaf("", class = Syntax, summary = "A name failed to bind.")]
    #[error("Validation error: {message}")]
    General { message: String },

    /// The name does not exist in the current namespace. Check spelling,
    /// the mounted namespace prefix (ns.table), and whether the relation
    /// needs a mount!/consult! first.
    #[leaf("table", class = Syntax, summary = "A named table (or relation) was not found.")]
    #[error("Table not found: {table}")]
    Table { table: String, context: String },

    /// The column does not exist in the relation's schema at this pipeline
    /// stage. Note that |> projection changes the visible columns: a filter
    /// AFTER |> (a, b) sees only a and b.
    #[leaf("column", class = Syntax, summary = "A named column was not found in scope.")]
    #[error("Column not found: {column}")]
    Column { column: String, context: String },

    /// `$.x` reads the scalar formal `x` of a relational or effect
    /// higher-order clause the reference stands in, nearest first. None
    /// declares `x`: the search never reaches a relation's columns, a
    /// namespace or a caller, and a value function's or lambda's bare
    /// parameter is not a marked formal.
    #[leaf("parameter", class = Syntax, summary = "A `$.x` reference names no enclosing higher-order formal.")]
    #[error("Validation error: {message}")]
    Parameter { message: String },

    /// After a join, an unqualified column name exists on more than one
    /// side. Qualify it with the relation alias (u.id).
    #[leaf("ambiguous", class = Syntax, summary = "A name matches more than one column in scope.")]
    #[error("Validation error: {message}")]
    Ambiguous { message: String },

    /// Filters and other column-reading operations bind authored names to
    /// column identities. An opaque passthrough relation or unknown
    /// table-valued function may be carried without a heading, but it
    /// cannot be filtered until its columns are introspectable. Check the
    /// relation spelling or make its schema available.
    #[leaf("schema", class = Syntax, summary = "A relation has no structural schema for name binding.")]
    #[error("Validation error: {message}")]
    Schema { message: String },

    /// DelightQL's catalog is closed and the target engine's callable
    /// surface is open: an UNQUALIFIED call that selects no DQL entity
    /// receives the default target transpilation, caveat emptor. A
    /// QUALIFIED name states where the callable lives in DelightQL's own
    /// world, so a miss refuses instead of guessing. To call the target
    /// engine's function explicitly, write sys::target.name:(args).
    #[leaf("callable_unknown", class = Syntax, summary = "A qualified callable name that no DQL entity answers.")]
    #[error("Validation error: {message}")]
    CallableUnknown { message: String },

    /// A `_ -> outputs` arm makes a fact function total over an unbounded
    /// input domain. The family is callable only: call it with
    /// `name:(inputs)`, or map that call over a separately supplied finite
    /// relation. Without a default, the explicit arms remain a finite
    /// relational face; an explicit `null -> outputs` arm is one ordinary
    /// finite row.
    #[leaf("fact_function/relational_face", class = Syntax, summary = "A default-bearing fact function was used as a relation.")]
    #[error("Validation error: {message}")]
    FactFunctionRelationalFace { message: String },

    /// The higher-order family: how arguments and piped relations land at a
    /// functor's parameters. Every parameter is inbound and must be supplied
    /// before the body opens. Members include incomplete_application,
    /// pipe_landing, and the closed relation/rule-value actual refusals.
    #[family("ho", summary = "A higher-order call's shape is ill-formed.")]
    #[error(transparent)]
    Ho(Ho),

    /// The trailing access group on a parameterized-rule call is ordinary
    /// argumentative access over the declared heading: bare names bind, _
    /// discards, a repeated name self-unifies (null-safe), a literal
    /// filters. Other element shapes (qualified references, expressions)
    /// are not access patterns — compute in a pipe stage instead.
    #[leaf("ho_access/pattern_shape", class = Syntax, summary = "An access-pattern element has an unsupported shape.")]
    #[error("Validation error: {message}")]
    HoAccessPatternShape { message: String },

    /// A pipe stage publishes a relation with no authored name, and `_`
    /// POINTS at it (LVARS.md). It is deixis, not a name: it performs no
    /// name lookup, and it requires exactly one visible unnamed pipe output.
    #[family(
        "pipe",
        summary = "The deictic `_` did not select exactly one unnamed pipe output."
    )]
    #[error(transparent)]
    Pipe(Pipe),

    /// Correlation references after a union-flavored operator address the
    /// operands' own headings — pads are output shape, never addressable.
    #[family(
        "setop",
        summary = "A set-operation correlation reference is ill-owned."
    )]
    #[error(transparent)]
    Setop(ResolutionSetop),

    /// Binding into an anonymous table: its header row, membership, and the
    /// qualifier and witness shapes it admits.
    #[family("anon", summary = "An anonymous table could not be bound as written.")]
    #[error(transparent)]
    Anon(AnonBinding),

    /// A context marker (`::ctx`) stood in a position that takes none.
    #[leaf("context/marker_position", class = Syntax, summary = "A context marker stood where none is admitted.")]
    #[error("Validation error: {message}")]
    ContextMarkerPosition { message: String },

    /// A predicate in a correlated position reads no column of the row it
    /// is correlated with, so it constrains nothing about that row.
    #[leaf("correlation/uncorrelated_predicate", class = Syntax, summary = "A correlated predicate reads nothing of its row.")]
    #[error("Validation error: {message}")]
    CorrelationUncorrelatedPredicate { message: String },

    /// A correspondence must be exact: every position on one side answers
    /// exactly one on the other. The message names the position that does
    /// not.
    #[leaf("correspondence/not-exact", class = Syntax, summary = "A correspondence is not exact.")]
    #[error("Validation error: {message}")]
    CorrespondenceNotExact { message: String },

    /// The live semantic relation environment could not answer for a scope
    /// a qualified reference named: the relation the scope stood for is no
    /// longer in the environment.
    #[leaf("scope/stale", class = Syntax, summary = "A qualified reference named a scope no longer live.")]
    #[error("Validation error: {message}")]
    ScopeStale { message: String },
}

/// `semantic/resolution/ho/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Resolution, Resolution::Ho))]
pub enum Ho {
    /// The parameter row is entirely inbound. Supply every scalar, relation,
    /// and rule-value parameter exactly once before the body opens, and
    /// supply no surplus members. A bare scalar name is an actual only when
    /// it resolves to one exact caller value; clause-head literals, binders,
    /// and same-spelled body columns do not supply it and are never
    /// published as an omitted argument.
    #[leaf("incomplete_application", class = Syntax, summary = "A higher-order application did not exactly complete its parameter row.")]
    #[error("Validation error: {message}")]
    IncompleteApplication { message: String },

    /// R8, strict: a piped relation lands at the FIRST formal parameter, or
    /// at exactly one explicit @ — never a search for a table parameter
    /// elsewhere, never displacement of a supplied argument. If the first
    /// parameter is already occupied, say where the pipe goes: f("arg", @).
    /// Two @ placeholders refuse — one pipe, one landing.
    #[leaf("pipe_landing", class = Syntax, summary = "The pipe's landing at this call is ill-formed.")]
    #[error("Validation error: {message}")]
    PipeLanding { message: String },

    /// A relation-valued argument is a CLOSED relation value: a whole named
    /// relation or parameterized application, an anonymous relation of any
    /// degree (one column included), or an explicit interior over its own
    /// source. An argumentative access — `f(users(_, dept, name))` — is not
    /// one: its names are logical binders, and letting them leak or turn
    /// private would change the access law. Construct the relation with a
    /// closed interior, `users(, cond |> (cols))`, or bind it first with `:`
    /// and pass the whole named relation.
    #[leaf("relation_actual_form", class = Syntax, summary = "A higher-order relation actual is not a closed relation value.")]
    #[error("Validation error: {message}")]
    RelationActualForm { message: String },

    /// A relation actual is closed: its interior may read its own source
    /// columns, literals and the statement's definitions, never a caller
    /// lvar, a sibling member's column, or a caller qualifier. Pass the
    /// value as an ordinary argument and read it inside the definition, or
    /// construct and name the relation first.
    #[leaf("relation_actual_capture", class = Syntax, summary = "A higher-order relation actual reads the calling row.")]
    #[error("Validation error: {message}")]
    RelationActualCapture { message: String },

    /// A relational argument stood where the parameter row takes a scalar
    /// or rule value, or a scalar stood where a relation is declared.
    #[leaf("relational_argument", class = Syntax, summary = "An argument's kind does not match its parameter's.")]
    #[error("Validation error: {message}")]
    RelationalArgument { message: String },

    /// A lifted call (`f(rows & …)`) crossed the boundary of the group it
    /// may lift within.
    #[leaf("lifted_boundary", class = Syntax, summary = "A lift crossed its group boundary.")]
    #[error("Validation error: {message}")]
    LiftedBoundary { message: String },

    /// A residual rule value captured a caller occurrence it may not read
    /// after closing.
    #[leaf("residual-capture", class = Syntax, summary = "A residual rule value captured a caller occurrence.")]
    #[error("Validation error: {message}")]
    ResidualCapture { message: String },

    /// A residual rule value was completed by rows built from its
    /// construction rows by an operation that no longer says which
    /// construction row each of them is.
    #[leaf("residual-completion", class = Syntax, summary = "A residual rule value was completed by rows that lost their construction rows.")]
    #[error("Validation error: {message}")]
    ResidualCompletion { message: String },

    /// A residual rule value's signature does not match the contract the
    /// consuming position demands: its remaining inputs, their modes, or
    /// its published heading.
    #[leaf("residual-contract", class = Syntax, summary = "A residual rule value does not satisfy its position's contract.")]
    #[error("Validation error: {message}")]
    ResidualContract { message: String },

    /// A residual rule value's remaining relation input was supplied from a
    /// frontier the value may not read.
    #[leaf("residual-frontier", class = Syntax, summary = "A residual rule value read a frontier it may not.")]
    #[error("Validation error: {message}")]
    ResidualFrontier { message: String },

    /// A residual rule value's remaining inputs were supplied out of the
    /// order its signature declares.
    #[leaf("residual-order", class = Syntax, summary = "A residual rule value's inputs were supplied out of order.")]
    #[error("Validation error: {message}")]
    ResidualOrder { message: String },

    /// The complete left prefix a residual rule value seals must be
    /// supplied; a gap in it leaves nothing to close.
    #[leaf("residual-prefix", class = Syntax, summary = "A residual rule value's sealed prefix is incomplete.")]
    #[error("Validation error: {message}")]
    ResidualPrefix { message: String },

    /// A residual rule value stood in a role its signature does not
    /// declare: a scalar where a relation was closed, or the reverse.
    #[leaf("residual-role", class = Syntax, summary = "A residual rule value stood in the wrong role.")]
    #[error("Validation error: {message}")]
    ResidualRole { message: String },

    /// A rule-valued formal was used as something other than a rule value
    /// inside the body that declares it.
    #[leaf("rule-formal", class = Syntax, summary = "A rule-valued formal was misused in its body.")]
    #[error("Validation error: {message}")]
    RuleFormal { message: String },

    /// A rule value must be pure; an effect rule cannot close into one.
    #[leaf("rule-value-effect", class = Syntax, summary = "An effect rule cannot be a rule value.")]
    #[error("Validation error: {message}")]
    RuleValueEffect { message: String },

    /// A rule-valued actual must be a rule reference or a closed residual
    /// application; the form written is neither.
    #[leaf("rule-value-form", class = Syntax, summary = "A rule-valued actual is not a rule value.")]
    #[error("Validation error: {message}")]
    RuleValueForm { message: String },

    /// A rule-valued parameter received no actual.
    #[leaf("rule-value-missing", class = Syntax, summary = "A rule-valued parameter was not supplied.")]
    #[error("Validation error: {message}")]
    RuleValueMissing { message: String },
}

/// `semantic/resolution/pipe/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Resolution, Resolution::Pipe))]
pub enum Pipe {
    /// No pipe has run at this point, so there is nothing for `_` to point
    /// at. This is not a misspelled qualifier — `_` never names anything, so
    /// there is no name to have got wrong. Write the relation's own name
    /// (users.id), or pipe first. A pipe whose output was named with `as`
    /// is no longer unnamed and is reached by that name instead.
    #[leaf("no_unnamed_pipe", class = Syntax, summary = "`_` was written where no unnamed pipe output is in view.")]
    #[error("Validation error: {message}")]
    NoUnnamedPipe { message: String },

    /// One spelling cannot stand for two relations, and a writer who meant a
    /// particular one has no way to say so. Name one of the stages with
    /// `as` — an alias replaces the anonymous form, leaving exactly one
    /// thing for `_` to point at.
    #[leaf("two_unnamed_pipes", class = Syntax, summary = "`_` was written with more than one unnamed pipe output in view.")]
    #[error("Validation error: {message}")]
    TwoUnnamedPipes { message: String },
}

/// `semantic/resolution/setop/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Resolution, Resolution::Setop))]
pub enum ResolutionSetop {
    /// A correlation after a union-flavored operator addresses the
    /// OPERANDS' OWN HEADINGS — the NULL pads of the corresponding output
    /// shape are output artifacts, never addressable. Every reference must
    /// belong to exactly one operand: a qualified reference's qualifier
    /// must name an operand (table name, alias, or answering name), and a
    /// bare reference's column must be carried by exactly one operand's
    /// heading. A bare name both operands carry is ambiguous — qualify it
    /// (x.col = y.col) to say which side it addresses.
    #[leaf("correlation_owner", class = Syntax, summary = "A set-operation correlation reference has no clear operand owner.")]
    #[error("Validation error: {message}")]
    CorrelationOwner { message: String },

    /// Minus is an exact name-aligned anti-match: every dimension on the
    /// left answers exactly one on the right and the reverse. Two operands
    /// of different widths, a pairing that is not one-to-one, or an operand
    /// whose heading cannot be enumerated leave nothing to anti-match on.
    /// Declare the dimensions at the mention so both operands publish the
    /// same exact heading.
    #[leaf("minus_heading", class = Syntax, summary = "A minus's two operands do not publish the same exact heading.")]
    #[error("Validation error: {message}")]
    MinusHeading { message: String },

    /// A correlation shared by both arms of a set operation would bind one
    /// reference to two operands.
    #[leaf("correlation/shared", class = Syntax, summary = "A set-operation correlation is shared by both arms.")]
    #[error("Validation error: {message}")]
    CorrelationShared { message: String },
}

/// `semantic/resolution/anon/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Resolution, Resolution::Anon))]
pub enum AnonBinding {
    /// An anonymous table's header row carries a logical variable; the
    /// header names columns, and a binder there has nothing to bind.
    #[leaf("header_row_lvar", class = Syntax, summary = "A logical variable stood in an anonymous header row.")]
    #[error("Validation error: {message}")]
    HeaderRowLvar { message: String },

    /// A membership test against an anonymous table cannot alias the table.
    #[leaf("membership_alias", class = Syntax, summary = "An anonymous membership target was aliased.")]
    #[error("Validation error: {message}")]
    MembershipAlias { message: String },

    /// A qualified reference named an anonymous table that publishes no
    /// such name.
    #[leaf("qualifier", class = Syntax, summary = "A qualifier named an anonymous table wrongly.")]
    #[error("Validation error: {message}")]
    Qualifier { message: String },

    /// A witness (`+`/`\+`) over an anonymous table must have the witness
    /// shape: one relation access, not a compound.
    #[leaf("witness_shape", class = Syntax, summary = "An anonymous witness is not witness-shaped.")]
    #[error("Validation error: {message}")]
    WitnessShape { message: String },
}
