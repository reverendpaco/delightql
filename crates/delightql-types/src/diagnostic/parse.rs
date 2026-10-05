// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `parse/…` — the source text could not be read.

use super::Taxon;

/// The parse family. The grammar refused the text; a member names the rule
/// the author broke where the failed parse's token stream shows one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Parse))]
pub enum Parse {
    /// The grammar rejected the text and no more specific parse category
    /// applied. The caret in the message marks the first unreadable token.
    #[leaf("general", class = Syntax, summary = "Generic parse failure.")]
    #[error("Parse error: {message}")]
    General { message: String },

    /// The text of a definition — a consulted rules file, a view/rule body,
    /// or inline DDL — contains syntax the DDL grammar rejects. The message
    /// carries the offending line and tree-sitter's recovery note. Common
    /// causes: bare infix arithmetic or `%` needing parentheses
    /// (`(price * 2) > 0`, `(n % 2) = 0` — sigil collision), unbalanced
    /// delimiters, or query-mode clause syntax (`… : name`) used in a rules
    /// file where definition syntax (`name(*) :- …`) is required.
    #[leaf("ddl", class = Syntax, summary = "A definition (DDL) source failed to parse.")]
    #[error("Parse error: {message}")]
    Ddl { message: String },

    /// A sigil-introduced expression (the compact operator forms) parsed as
    /// structurally invalid. Check the sigil's expected operand shape and
    /// delimiter balance near the reported position.
    #[leaf("sigil", class = Syntax, summary = "A sigil expression contains syntax errors.")]
    #[error("Parse error: {message}")]
    Sigil { message: String },

    /// A submission holding several queries was sent where one statement is
    /// the contract. Send each query as its own term, or run the file through
    /// the sequential entrance.
    #[leaf("multi_query", class = Syntax, summary = "Several queries stood in a one-statement submission.")]
    #[error("Parse error: multi-query input rejected: found {count} queries in one submission (send each query separately, or run the file through the sequential entrance)")]
    MultiQuery { count: usize },

    /// A submission declared definitions beside the goal it runs. A
    /// submission either runs one goal or declares definitions, so a
    /// definition written after a query would otherwise have nowhere to go.
    /// Send the definitions as a submission of their own, or in a
    /// `(~~ddl ~~)` block beside the query.
    #[leaf("definitions_beside_goal", class = Syntax, summary = "Definitions stood beside a goal in one submission.")]
    #[error("Parse error: this submission declares {count} definition clause(s) beside its query; send the definitions as a submission of their own, or in a (~~ddl ~~) block")]
    DefinitionsBesideGoal { count: usize },

    /// A file that declares the query-sequence entrance was supplied where
    /// a definition library is required. Query sequences are executable
    /// programs, not consultable definitions.
    #[leaf("file_category", class = Syntax, summary = "A source file declares a category the consuming entrance cannot accept.")]
    #[error("Parse error: a query-sequence file cannot be consulted as a definition file")]
    FileCategory,

    /// One goal declares one expected error. A second `(~~error … ~~)` on
    /// the same goal leaves the runner unable to say which was meant.
    #[leaf("error_hook/repeated", class = Syntax, summary = "One goal declared two expected errors.")]
    #[error("Parse error: one goal declares one expected error; this one declares two")]
    ErrorHookRepeated,

    /// An `(~~error://… ~~)` hook named a path that is neither a family, an
    /// emitted identity, a retired alias, nor a validated provider tail.
    /// The declared path is a typo: `dql explain` knows every lawful
    /// selector.
    #[leaf("error_hook/unknown", class = Syntax, summary = "An expected-error hook named an unknown path.")]
    #[error("Parse error: {message}")]
    ErrorHookUnknown { message: String },

    /// A `(~~danger://… ~~)` annotation named a gate the compiler does not
    /// register. The message lists the known gates.
    #[leaf("danger/unknown", class = Syntax, summary = "A danger annotation named an unknown gate.")]
    #[error("Parse error: {message}")]
    DangerUnknown { message: String },

    /// A `(~~config://… ~~)` annotation named a setting the compiler does not
    /// register. The message lists the known settings.
    #[leaf("config/unknown", class = Syntax, summary = "A config annotation named an unknown setting.")]
    #[error("Parse error: {message}")]
    ConfigUnknown { message: String },

    /// A string template carried an escape sequence the template grammar
    /// does not admit.
    #[leaf("template/escape", class = Syntax, summary = "A template escape sequence is not admitted.")]
    #[error("Parse error: {message}")]
    TemplateEscape { message: String },

    /// The typed syntax tree carried a node the normalizer takes no position
    /// on: the grammar admits a shape whose reading into the AST is not yet
    /// written. The message names the node.
    #[leaf("normalize/gap", class = Syntax, summary = "The grammar admits a shape normalization cannot yet read.")]
    #[error("Parse error: {message}")]
    NormalizeGap { message: String },

    /// A function-shaped form — a value function, lambda, or window call —
    /// was written in a shape the language does not admit: wrong argument
    /// count for a fixed-arity form, a spelling that is not a function
    /// application, or a call where a name must stand.
    #[leaf("function", class = Syntax, summary = "A function form is ill-shaped.")]
    #[error("Parse error: {message}")]
    Function { message: String },

    /// The anonymous table literal `_(…)`: its header and rows must agree,
    /// and it must hold something.
    #[family("anon", summary = "An anonymous table literal is ill-shaped.")]
    #[error(transparent)]
    Anon(ParseAnon),

    /// DelightQL has NO operator precedence: `a * b + c` has no reading,
    /// because the language refuses to rank `*` over `+` (the PONY rule),
    /// and `a or b and c` has none, because it refuses to rank the truth
    /// connectives too. Every composition is grouped explicitly —
    /// `((a * b) + c)` or `(a * (b + c))`, `(a or b) and c` or
    /// `a or (b and c)` — so the meaning is always on the page. A mixed
    /// connective run is recognized by the grammar as a refusal witness and
    /// refused here at normalization; the parser cannot accept the other
    /// ungrouped forms even to complain about them, and their diagnosis is
    /// recovered from the failed parse's token stream.
    #[leaf("pony", class = Syntax, summary = "Mixed operators without grouping (no PEMDAS).")]
    #[error("Parse error: {message}")]
    Pony { message: String },

    /// There is no `is null` / `is not null` operator. `=` is the null-safe
    /// equality (compiles to IS NOT DISTINCT FROM), so `col = null` is the
    /// null check and `col != null` its negation.
    #[leaf("is_null", class = Syntax, summary = "SQL `is null` used; DelightQL spells it `= null`.")]
    #[error("Parse error: {message}")]
    IsNull { message: String },

    /// The anonymous table constructor is ONE token: `_(id @ 1)`. With a
    /// space (`_ (id @ 1)`) the parser sees a discard followed by a
    /// parenthesized expression and rejects the statement. Remove the space.
    #[leaf("anon_space", class = Syntax, summary = "Space between `_` and `(` in an anonymous table.")]
    #[error("Parse error: {message}")]
    AnonSpace { message: String },

    /// There is no `--` line comment (and no `/* */` block comment): `--`
    /// lexes as two `-` operators and breaks the parse. The line comment is
    /// `//`, in both query mode and rules files. Inside a string literal
    /// `--` is ordinary text. If subtraction of a negative was meant, group
    /// it explicitly: `a - (-b)`. This diagnosis is recovered from the
    /// failed parse's token stream.
    #[leaf("comment", class = Syntax, summary = "SQL `--` comment used; DelightQL comments are `//`.")]
    #[error("Parse error: {message}")]
    Comment { message: String },

    /// There is no `#(-col)` descending shorthand. Descending is spelled per
    /// key with `desc`: `#(col desc)`, `#(a desc, b)` — in pipe sorts and
    /// window specs alike. A unary minus meant as arithmetic needs explicit
    /// grouping: `#((0 - col))`. This diagnosis is recovered from the failed
    /// parse's token stream.
    #[leaf("sort_minus", class = Syntax, summary = "Minus-prefix descending sort; the spelling is `col desc`.")]
    #[error("Parse error: {message}")]
    SortMinus { message: String },

    /// `==` and `!==` are no longer DelightQL syntax. DelightQL equality is
    /// `=` and inequality `!=`, both null-safe. The target engine's own
    /// three-valued comparison — unknown on null, target coercions and
    /// collations — is the explicit prelude sigma predicate: `+sql_eq(l, r)`
    /// lowers to SQL `=` and `+sql_ne(l, r)` to SQL `<>`. Choose by intent:
    /// a filter or join relationship that is not about the engine's null
    /// answer migrates to `=`; a fixture that deliberately asks the engine
    /// becomes `+sql_eq(l, r)`. This diagnosis is recovered from the failed
    /// parse's token stream.
    #[leaf("retired_operator", class = Syntax, summary = "Retired `==` / `!==` glyph; DelightQL equality is `=`, target SQL equality is `+sql_eq`.")]
    #[error("Parse error: {message}")]
    RetiredOperator { message: String },

    /// Session directives (mount!, consult!, enlist!, and their kin) change
    /// what the compilation can see, so they are legal at the REPL/CLI top
    /// level or in a liminal program — never nested in a data position,
    /// where their ordering relative to the query around them would be
    /// undefined.
    #[leaf("session_position", class = Syntax, summary = "A session directive stood inside a query.")]
    #[error("Parse error: {message}")]
    SessionPosition { message: String },

    /// Define a pure property rule and demand assert!(property)(*) on the
    /// relation being checked. Assertions are ordinary effects; the
    /// annotation sidecar no longer exists.
    #[leaf("assertion/retired", class = Syntax, summary = "The retired assertion annotation was written.")]
    #[error("Parse error: {message}")]
    AssertionRetired { message: String },

    /// `"key": ~>` promises an interior TABLE — an array per group — while a
    /// metadata group yields an interior RECORD, one object per group. A
    /// metadata group stands under a fixed key by its own spelling: in a
    /// PATTERN `"key": ~> c:~> {…}` (or the braced nesting
    /// `"key": { c:~> {…} }`), and in a CONSTRUCTION `"key": c:~> {…}` — the
    /// group directly, with no second induction between.
    #[leaf("metadata_induction", class = Syntax, summary = "A metadata group was induced under a data key.")]
    #[error("Parse error: {message}")]
    MetadataInduction { message: String },

    /// There is one accessor door and it takes exactly ONE path, spelled
    /// with its steps: `x:{.a.b}`. A path is spec, not a value — it never
    /// evaluates alone and nothing produces one at runtime, so a bare name
    /// inside the braces can never be fed. `"$…"` is the target engine's own
    /// path sub-language and stays with the target. `x:[1]` says the same
    /// thing as `x:{.1}` with a shape that reads as a type: an accessor
    /// reads, so it takes the one path spelling.
    #[leaf("path_variable", class = Syntax, summary = "The json accessor was handed something other than one literal path.")]
    #[error("Parse error: {message}")]
    PathVariable { message: String },

    /// A relational rule's body is a relex and an effect rule's is an
    /// effrelex, so a head without `!` whose body demands a directive has
    /// no derivation. Declare the effect in the head — `name!(*) :- …` — or
    /// take the directive out of the body.
    #[leaf("effect/purity", class = Syntax, summary = "A pure head carried an effectful body.")]
    #[error("Parse error: {message}")]
    EffectPurity { message: String },

    /// Under an effect head the law admits a directive inside a predicate
    /// subquery; what is missing is its lowering, so nothing derives there
    /// yet. Lift it out of the predicate and demand it as its own step.
    #[leaf("directive/position", class = Syntax, summary = "A directive stood where only a relation derives.")]
    #[error("Parse error: {message}")]
    DirectivePosition { message: String },

    /// `$.x` reads the scalar formal `x` of an enclosing relational or
    /// effect higher-order clause. It is one glued form — the sigil and the
    /// formal's name, no space between — and it is a value: a formal is
    /// declared bare in its head, and a reference names, declares and
    /// addresses no column. A bound or an ordinal reads a formal as `$.n`.
    #[leaf("parameter_reference", class = Syntax, summary = "A `$.x` parameter reference is malformed or out of place.")]
    #[error("Parse error: {message}")]
    ParameterReference { message: String },

    /// A label ASSERTS what its body is. A binding whose body demands a
    /// directive is an effect binding, so its label carries the mark: write
    /// `: name!`.
    #[leaf("effect/label", class = Syntax, summary = "An effectful body was bound under a pure label.")]
    #[error("Parse error: {message}")]
    EffectLabel { message: String },

    /// Structural head grounding is reserved. A head parameter names a
    /// relation or a scalar; destructuring a shape in a head has no
    /// derivation.
    #[leaf("structural_head", class = Syntax, summary = "A head parameter named a shape to destructure.")]
    #[error("Parse error: {message}")]
    StructuralHead { message: String },

    /// DelightQL has no operator precedence, and a bare `%` reads as the
    /// group-modulo sigil wherever a relational reading is possible.
    /// Parenthesize the arithmetic: `f:(n | (n % 2) = 0)`.
    #[leaf("guard_grouping", class = Syntax, summary = "A guard composed operators without grouping.")]
    #[error("Parse error: {message}")]
    GuardGrouping { message: String },

    /// How many groups follow the name decides what the first one is. With
    /// ONE group, `f(*)` is ordinary access and the glob names the whole
    /// heading. With TWO, the left group supplies the callee's parameters,
    /// and `*` names no actual for any parameter — the same glyph does not
    /// mean access in one left group and an unspecified value in another.
    /// Supply every scalar, relation, and rule-value actual; use an ordinary
    /// relation when those positions must be enumerable.
    #[leaf("glob_argument", class = Syntax, summary = "A bare glob stood where a higher-order argument stands.")]
    #[error("Parse error: {message}")]
    GlobArgument { message: String },

    /// A head is an ordered projection of its body's heading: each position
    /// holds a name or a ground term, optionally labeled with `as`. A call
    /// or expression computes, and a computation is not a name — a head
    /// that computes is not a head. Compute in the body and label the
    /// result: `h(*) : body |> (count:(a) as n)`.
    #[leaf("head_computes", class = Syntax, summary = "A defining head contained a computation.")]
    #[error("Parse error: {message}")]
    HeadComputes { message: String },

    /// `&` bounds arguments only in a two-group call, where the lifted rows
    /// follow it and dissolve into an anonymous-table argument
    /// (`f(users(*) & 1, 2)(*)`). A one-group call's parentheses are its
    /// arguments alone, so a `&` tail there has no meaning. The message
    /// rewrites the call: a tail of bare names is a projection, which
    /// belongs to the ACCESS group (`json_each(doc, path)(value, type)`);
    /// a tail of values is lifted rows, and the call needs its access group
    /// after them (`like_any(x & "a"; "b")(*)`).
    #[leaf("lift_tail", class = Syntax, summary = "A lift tail stood in a one-group call.")]
    #[error("Parse error: {message}")]
    LiftTail { message: String },

    /// A definition's body is ONE domain expression. `as` names a
    /// publication position — a projection item, an embed, a stage — and a
    /// parenthesized list of named values is a row, which a value definition
    /// does not produce. Publish the columns from the caller's projection
    /// instead, applying the function per column.
    #[leaf("value_naming", class = Syntax, summary = "A definition's body was a row of named values.")]
    #[error("Parse error: {message}")]
    ValueNaming { message: String },

    /// Iteration derives a record or a tuple to destructure into; a bare
    /// name names nothing to destructure. To bind each plain value of an
    /// array, write the binder inside brackets: `"key": ~> [v]`.
    #[leaf("iteration_binder", class = Syntax, summary = "A bare iteration binder has no derivation.")]
    #[error("Parse error: {message}")]
    IterationBinder { message: String },

    /// A pattern extracts values; a qualified name in a pattern member would
    /// assert an equality with an existing column instead, and that is not
    /// what patterns do. Reach into the document with a path binding —
    /// `.person.first` publishes `person_first`, and `as` renames.
    /// Construction position may qualify freely.
    #[leaf("pattern_qualified", class = Syntax, summary = "A pattern member cannot be qualified.")]
    #[error("Parse error: {message}")]
    PatternQualified { message: String },

    /// A column regex ignores case by default, as a column reference does,
    /// so `i` restates it: one spelling per meaning. The one flag is `c`,
    /// which makes the match case-sensitive: `/Date/c`.
    #[leaf("regex_ignore_case", class = Syntax, summary = "A column regex was flagged `i`, which is already its default.")]
    #[error("Parse error: {message}")]
    RegexIgnoreCase { message: String },

    /// A column regex takes one flag, `c`, glued to its closing slash; any
    /// other letter there is not a flag.
    #[leaf("regex_flag", class = Syntax, summary = "A column regex carried an unknown flag.")]
    #[error("Parse error: {message}")]
    RegexFlag { message: String },

    /// The mixed argument list does not embed the relation grammar: a set
    /// expression, a pipeline, or a join has no derivation inside `f(…)`.
    /// Bind the relation first — `… : name` — and pass the whole named
    /// access, `f(name(*))(*)`. This diagnosis is recovered from the failed
    /// parse's token stream.
    #[leaf("ho/relation_actual", class = Syntax, summary = "A compound relation expression stands in a higher-order argument list.")]
    #[error("Parse error: {message}")]
    HoRelationActual { message: String },
}

/// `parse/anon/…` — the family's own path is the general ill-shaped literal.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Parse, Parse::Anon))]
pub enum ParseAnon {
    /// An anonymous table literal (`_(…)`) is ill-shaped: its header and rows
    /// disagree in width, a row is wider than the header, or the form mixes
    /// header and row spellings.
    #[leaf("", class = Syntax, summary = "An anonymous table literal is ill-shaped.")]
    #[error("Parse error: {message}")]
    General { message: String },

    /// `_()` names no relation: it has no columns and no rows, so there is
    /// nothing for it to be. It is not the union identity either — that is
    /// the empty relation OF THE MATCHING SCHEMA, whose typed spelling
    /// (`_(cols @)`) is reserved and not yet available. Write the relation
    /// you mean: `_(id @ 1)` for a row, or a header form `_(a, b ---- 1, 2)`.
    /// This diagnosis is recovered from the failed parse's token stream.
    #[leaf("empty", class = Syntax, summary = "There is no empty anonymous table.")]
    #[error("Parse error: {message}")]
    Empty { message: String },

    /// A bare singleton `name@value` is the anonymous table of ONE row and
    /// one column, so it has no second row to separate. Several rows are
    /// the wrapped form: `_(a @ 1; 2)`. The grammar recognizes the extra
    /// rows as a refusal witness, so the whole statement is the one refused.
    #[leaf("singleton_rows", class = Syntax, summary = "A bare singleton was given several rows.")]
    #[error("Parse error: {message}")]
    SingletonRows { message: String },

    /// The name left of a bare singleton's `@` is the column it publishes,
    /// and a published column carries no qualifier. Name the singleton's
    /// table with `as`: `a@2 as g` publishes `g.a`. To test an existing
    /// qualified position, compare it: `g.a = 2`.
    #[leaf("singleton_qualified", class = Syntax, summary = "A bare singleton named a qualified column.")]
    #[error("Parse error: {message}")]
    SingletonQualified { message: String },

    /// The word left of a bare singleton's `@` is the column it publishes.
    /// `true`, `false` and `null` are literals there, as they are in a
    /// written header, where `_(true @ 2)` is a row that must equal the
    /// value rather than a column named `true`. Strop the word to publish a
    /// column of that name: `` `true`@2 ``.
    #[leaf("singleton_literal", class = Syntax, summary = "A bare singleton's column was a literal word.")]
    #[error("Parse error: {message}")]
    SingletonLiteral { message: String },
}
