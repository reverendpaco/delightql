// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `semantic/…` — the query is well-formed but does not mean what it must.

mod ddl;
mod directive;
mod effect;
mod grounding;
mod recursion;
mod resolution;
mod shapes;

pub use ddl::{Ddl, DdlHead};
pub use directive::{Directive, DirectiveBinding, DirectiveContext};
pub use effect::{
    Effect, EffectBin, EffectBody, EffectCte, EffectDdl, EffectMain, EffectPipe, EffectPlan,
    EffectRule, EffectRun,
};
pub use grounding::{Er, Ground, Grounding};
pub use recursion::Recursion;
pub use resolution::{AnonBinding, Ho, Pipe, Resolution, ResolutionSetop};
pub use shapes::{
    Anon, Cfe, Compression, Constraint, Cte, Expansion, FactFunction, HoDefinition, Identifier,
    Interior, Join, Landing, Limitation, Mention, Mode, Narrowing, Set, SetOperation, Setop,
    Transform, Using, Window,
};

use super::Taxon;

/// The semantic family.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Semantic))]
pub enum Semantic {
    /// Resolution errors are name-binding failures: a table, column, alias,
    /// or qualifier that does not name anything in scope, or names two
    /// things at once (ambiguous). Members include table, column, ambiguous,
    /// ho/ (higher-order landing and access), setop/ (set-operation
    /// correlation ownership), anon/ (anonymous-table binding), pipe/ (the
    /// deictic `_`).
    #[family("resolution", summary = "A name failed to bind.")]
    #[error(transparent)]
    Resolution(Resolution),

    /// The general validation family: the query parsed and its names
    /// resolved, but a rule of the language refused the composition — a
    /// shape a road cannot carry, an option that contradicts another, a
    /// structure the release does not support. Where a violation has its
    /// own identifier the message carries the more specific badge;
    /// semantic/constraint is the family every remaining validation refusal
    /// reports under. Hook family: (~~error://semantic/constraint ~~).
    #[family(
        "constraint",
        summary = "A compilation-time rule of the language was violated."
    )]
    #[error(transparent)]
    Constraint(Constraint),

    /// A function, predicate, or POSITIONAL TABLE PATTERN received the wrong
    /// number of arguments. The commonest case: a table access like
    /// users(name, age) is not a projection — it is a positional pattern
    /// that must supply one slot per column (full arity). Fill unwanted
    /// positions with _ (users(_, name, _)) or keep everything with *. For
    /// functions and rules, the message names the exact expected/actual
    /// counts.
    #[leaf("arity", class = Syntax, summary = "Wrong number of arguments.")]
    #[error("Validation error: {message}")]
    Arity { message: String },

    /// cast:(expr, type) takes a bare type name from the v1 vocabulary:
    /// integer, real, text, numeric, boolean. Target engines apply their own
    /// cast semantics (Postgres rounds real→integer; SQLite truncates) — see
    /// the book's cast page.
    #[leaf("cast", class = Syntax, summary = "Invalid cast:() usage.")]
    #[error("Validation error: {message}")]
    Cast { message: String },

    /// Pathing ('col:{.field}', 'col:[0]') reaches into a value; narrowing
    /// ('|> .col{.field}') iterates one. A column declared as a plain scalar
    /// (INTEGER, REAL, BOOLEAN, dates) has no insides to reach into and no
    /// rows to iterate — aiming these tools at it used to fail
    /// target-dependently at runtime. Refused at compile time instead. TEXT
    /// columns stay permissive: documents live in TEXT, and declarations
    /// cannot be trusted to deny it. Aim the tool at a compound value:
    /// something built with {...}/[...], a tree-group, or a document column.
    #[leaf("compound/scalar_column", class = Syntax, summary = "A compound-value tool aimed at a plainly-scalar column.")]
    #[error("Validation error: {message}")]
    CompoundScalarColumn { message: String },

    /// Family for refusals of recursive forms the language does not permit
    /// (RECURSION-CONTRACT.md). DelightQL recursion is a generator
    /// (co-recursion): each recursive clause sees only the previous
    /// iteration's rows — never the accumulated result, never itself as a
    /// callable. Forms outside that contract are refused here, each with
    /// its rewrite path.
    #[family(
        "recursion",
        summary = "A recursive definition breaks the recursion contract."
    )]
    #[error(transparent)]
    Recursion(Recursion),

    /// ground!'s own validation: the library and the data namespace it is
    /// bound to must not share names, and every qualified reference must
    /// resolve where it points.
    #[family(
        "ground",
        summary = "ground! refused the library it was asked to bind."
    )]
    #[error(transparent)]
    Ground(Ground),

    /// Family for the grounding/mention doctrine (GROUNDING-AND-MENTION.md):
    /// rule heads ground on literals and mentions by canonical spelling;
    /// contexts are symbols; edges are declared, finite, and selected by
    /// their terms' exact canonical spellings. Subhierarchy: head/ (clause
    /// selection), er/ (the entity-relationship operators & and &&), and
    /// data_hole_unbound (a consulted world's free data name).
    #[family(
        "grounding",
        summary = "A head-grounding or mention rule was violated."
    )]
    #[error(transparent)]
    Grounding(Grounding),

    /// What a load may and may not do: THE LIMINAL SPACE admits a read-only
    /// witness goal, canonically spelled, and declarations only where a
    /// road can spend them.
    #[family("consult", summary = "A consulted file broke the liminal law.")]
    #[error(transparent)]
    Consult(Consult),

    /// Write an integer literal, or bind the identifier to an integer scalar
    /// parameter in the active higher-order call. Missing bindings,
    /// fractional numbers, and values outside the integer range refuse
    /// instead of silently becoming zero.
    #[leaf("limit/value", class = Syntax, summary = "A limit or offset bound is not an integer value.")]
    #[error("Validation error: {message}")]
    LimitValue { message: String },

    /// Known gaps: forms the language admits whose implementation this
    /// release does not carry.
    #[family("limitation", summary = "A known limitation of this release.")]
    #[error(transparent)]
    Limitation(Limitation),

    /// How a name may be spelled: exact `_` is deixis, never an authored
    /// name. No word is reserved.
    #[family(
        "identifier",
        summary = "A name was spelled in a way the language reserves."
    )]
    #[error(transparent)]
    Identifier(Identifier),

    /// TWO LIVE SCOPES NEVER SHARE A NAME: when two relations in one lexical
    /// environment answer to the same canonical name — two members aliased
    /// `q`, or one table accessed twice bare — a qualified reference could
    /// name either, so the activation refuses before any consumer can
    /// choose. Give one of them its own name with `as`; no gate admits the
    /// shape.
    #[leaf("scope/duplicate", class = Syntax, summary = "Two live scopes share one answering name.")]
    #[error("Validation error: {message}")]
    ScopeDuplicate { message: String },

    /// An annotation name the language reserves was written where an
    /// authored annotation stands.
    #[leaf("annotation/reserved", class = Syntax, summary = "A reserved annotation name was written.")]
    #[error("Validation error: {message}")]
    AnnotationReserved { message: String },

    /// The anonymous table's sparse form: the header, the rows, and the
    /// fill positions must agree.
    #[family("anon", summary = "An anonymous table's sparse form is ill-formed.")]
    #[error(transparent)]
    Anon(Anon),

    /// Query-scoped value functions (CFEs): their arity, parameters,
    /// covers, and the recursion they may not perform.
    #[family(
        "cfe",
        summary = "A query-scoped value function was misdeclared or misused."
    )]
    #[error(transparent)]
    Cfe(Cfe),

    /// The compression operator's degree and base.
    #[family("compression", summary = "A compression is ill-formed.")]
    #[error(transparent)]
    Compression(Compression),

    /// The CTE family covers query-scoped rules declared with the : neck.
    #[family(
        "cte",
        summary = "A query-scoped rule (CTE) was mis-declared or misused."
    )]
    #[error(transparent)]
    Cte(Cte),

    /// Definition assembly: the clauses of one definition must agree, a
    /// group must have one subject, and one name names one entity.
    #[family("ddl", summary = "A definition's clauses or head are ill-formed.")]
    #[error(transparent)]
    Ddl(Ddl),

    /// Directive invocation: bindings, contexts, receipts, and the
    /// lifecycle verbs' own refusals.
    #[family("directive", summary = "A directive was invoked wrongly.")]
    #[error(transparent)]
    Directive(Directive),

    /// The receipt algebra's discipline: what an effect rule may contain,
    /// where a directive may stand, how a plan may be realized. Effect
    /// discipline IS a semantic error, so (~~error://semantic ~~) matches
    /// these too.
    #[family("effect", summary = "The receipt algebra's discipline was violated.")]
    #[error(transparent)]
    Effect(Effect),

    /// An edge declaration's chain form.
    #[leaf("er/chain/declaration", class = Syntax, summary = "An edge was declared with a chain the declaration form does not admit.")]
    #[error("Validation error: {message}")]
    ErChainDeclaration { message: String },

    /// Expansion of a definition body into its use site.
    #[family(
        "expansion",
        summary = "A definition could not be expanded at its use."
    )]
    #[error(transparent)]
    Expansion(Expansion),

    /// Fact functions: their names, widths, and the inputs their outputs
    /// read.
    #[family("fact_function", summary = "A fact function is ill-declared.")]
    #[error(transparent)]
    FactFunction(FactFunction),

    /// The clauses of one definition family disagree about their heads.
    #[leaf("heads/clause_disagreement", class = Syntax, summary = "A family's clauses disagree about their head.")]
    #[error("Validation error: {message}")]
    HeadsClauseDisagreement { message: String },

    /// Family for refusals in higher-order definition bodies. Higher-order
    /// scalar parameters bind by AST substitution at the call — a parameter
    /// name is a supplied value spliced into the body, never a column of it.
    #[family("ho", summary = "A higher-order view was parameterized wrongly.")]
    #[error(transparent)]
    Ho(HoDefinition),

    /// An inchoate (not yet manifested) name was read before anything
    /// published it.
    #[leaf("inchoate/latent_name", class = Syntax, summary = "A latent name was read before it was published.")]
    #[error("Validation error: {message}")]
    InchoateLatentName { message: String },

    /// The interior family covers relation-valued columns: drills, narrows,
    /// and interior-scoped operations.
    #[family("interior", summary = "An interior (nested) relation was misused.")]
    #[error(transparent)]
    Interior(Interior),

    /// Join spellings and their conditions.
    #[family("join", summary = "A join is ill-formed.")]
    #[error(transparent)]
    Join(Join),

    /// Where a piped relation lands: the binder and hole discipline.
    #[family("landing", summary = "A pipe landing is ill-formed.")]
    #[error(transparent)]
    Landing(Landing),

    /// The bootstrap store cannot hold a BLOB in a materialized companion
    /// relation.
    #[leaf("materialization/bootstrap_blob", class = Syntax, summary = "A BLOB cannot be materialized into the bootstrap store.")]
    #[error("Validation error: {message}")]
    MaterializationBootstrapBlob { message: String },

    /// The relational membership operator's operands must match in width.
    #[leaf("membership/arity", class = Syntax, summary = "Membership operands differ in width.")]
    #[error("Validation error: {message}")]
    MembershipArity { message: String },

    /// Mentions are names passed by spelling, never evaluated: :`delimited`,
    /// ::light, and functor terms in edge declarations.
    #[family("mention", summary = "A mention (uninterpreted name) was misused.")]
    #[error(transparent)]
    Mention(Mention),

    /// Declared modes of a fact function: their arity, degree, and outputs.
    #[family("mode", summary = "A declared mode is ill-formed.")]
    #[error(transparent)]
    Mode(Mode),

    /// Narrowing reads interior values out of JSON-carrying columns
    /// (JSON-SUBSTRATE.md): owned at release points, contained elsewhere;
    /// non-array and malformed values narrow to ZERO ROWS by the
    /// null-interior road. Members refuse narrowing aimed at plainly scalar
    /// columns or ill-formed object literals.
    #[family(
        "narrowing",
        summary = "A JSON narrowing operation was ill-typed or ill-aimed."
    )]
    #[error(transparent)]
    Narrowing(Narrowing),

    /// A qualifier with more than one segment where one namespace or
    /// relation name stands.
    #[leaf("reference/multi_segment_qualifier", class = Syntax, summary = "A qualifier carried more than one segment.")]
    #[error("Validation error: {message}")]
    ReferenceMultiSegmentQualifier { message: String },

    /// The refiner's existence rewrite met a condition it cannot inject
    /// into the interior it builds.
    #[leaf("refiner/exists/injection_condition", class = Syntax, summary = "An existence condition cannot be injected.")]
    #[error("Validation error: {message}")]
    RefinerExistsInjectionCondition { message: String },

    /// Set correlation: how the arms of a set expression may be named and
    /// correlated.
    #[family("set", summary = "A set expression's correlation is ill-formed.")]
    #[error(transparent)]
    Set(Set),

    /// Set operators over correspondence: ambiguity and the multiplicity
    /// gate.
    #[family(
        "setop",
        summary = "A set operator's correspondence is ambiguous or gated."
    )]
    #[error(transparent)]
    Setop(Setop),

    /// A positional set operation's operands must agree in column count
    /// and, where names align them, in names.
    #[family("set_operation", summary = "A set operation's operands do not align.")]
    #[error(transparent)]
    SetOperation(SetOperation),

    /// Lowering shapes the compiler cannot yet carry for a valid query.
    #[family(
        "transform",
        summary = "A valid form has no lowering on this road yet."
    )]
    #[error(transparent)]
    Transform(Transform),

    /// The `using all` join's shared-column discipline.
    #[family("using", summary = "A `using` join's columns are ill-determined.")]
    #[error(transparent)]
    Using(Using),

    /// An open value (a partially applied function) was read where a value
    /// stands; apply it fully first.
    #[leaf("value/open/unapplied", class = Syntax, summary = "An open value was read unapplied.")]
    #[error("Validation error: {message}")]
    ValueOpenUnapplied { message: String },

    /// Window calls and what may carry a window.
    #[family("window", summary = "A window call is ill-placed.")]
    #[error(transparent)]
    Window(Window),
}

/// `semantic/consult/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Semantic, Semantic::Consult))]
pub enum Consult {
    /// THE LIMINAL SPACE: loading a file may READ user data only, through a
    /// top-level goal (`?- body`) that runs in the consultation and records
    /// a YES/NO witness; it may never write. The grammar already bars a
    /// directive from a relational goal, so reaching this refusal means the
    /// goal's body lowered to a mutation — state the write as an effect rule
    /// and demand it after the load.
    #[leaf("witness/read_only", class = Syntax, summary = "A consulted goal compiled to a statement that writes.")]
    #[error("Validation error: {message}")]
    WitnessReadOnly { message: String },

    /// A sidecar belongs to the form that wrote it, and at LOAD there is one
    /// road that can spend one: a relational goal (`?- body`) compiles and
    /// executes, so its danger/config acknowledgments and subordinate blocks
    /// travel into its own compilation. A session directive compiles no
    /// query, so a declaration on one has no evaluator; and an
    /// expected-error hook has no meaning anywhere in a load, which ABORTS
    /// on failure rather than recording one. Move the declaration to the
    /// goal it is about, to file scope, or to the statement that demands
    /// the load.
    #[leaf("liminal/declaration", class = Syntax, summary = "A liminal statement declared something the load cannot spend.")]
    #[error("Validation error: {message}")]
    LiminalDeclaration { message: String },

    /// THE LIMINAL RELATION names each goal by its body's canonical
    /// spelling, so a ledger scan knows which goal was which across layout
    /// and reconsult. The canonicalizer is the format engine under its
    /// frozen default style; a body it passes through has no canonical
    /// identity, and keeping the authored bytes instead would be a second
    /// spelling authority that agrees with the first only by accident.
    #[leaf("goal/unspellable", class = Syntax, summary = "A consulted goal has no canonical spelling.")]
    #[error("Validation error: {message}")]
    GoalUnspellable { message: String },
}
