// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The smaller semantic families: constraints, forms, and shapes.

use super::Semantic;
use crate::diagnostic::{DelightQLError, Taxon};

/// `semantic/constraint/…` — its own path is an emitted identity.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Constraint))]
pub enum Constraint {
    /// The general validation refusal: a rule of the language refused the
    /// composition and no member names it more precisely. The message
    /// states the rule.
    #[leaf("", class = Syntax, summary = "A compilation-time rule of the language was violated.")]
    #[error("Validation error: {message}")]
    General { message: String },

    /// THE WRITTEN NAME IS THE NAMING. A slot binds by POSITION: a bare name
    /// binds a fresh column, a qualified one reuses an existing value, and
    /// a term constrains the column and is consumed. None of them publishes
    /// a name for `as` to change. Rename where names are published — in a
    /// projection: `f(…) |> (col as name)`.
    #[leaf("positional_alias", class = Syntax, summary = "An `as` alias stood in a positional slot.")]
    #[error("Validation error: {message}")]
    PositionalAlias { message: String },

    /// A pivot's keys, values, or column set are not what the pivot
    /// operation requires.
    #[leaf("pivot", class = Syntax, summary = "A pivot is ill-formed.")]
    #[error("Validation error: {message}")]
    Pivot { message: String },

    /// A destructuring pattern does not match the shape it destructures.
    #[leaf("destructuring", class = Syntax, summary = "A destructuring pattern does not match its value.")]
    #[error("Validation error: {message}")]
    Destructuring { message: String },

    /// A join spelling the language does not admit here: an outer join
    /// without a condition, a full outer join where the target has none, a
    /// join whose sides cannot be told apart.
    #[leaf("join", class = Syntax, summary = "A join is not admitted as written.")]
    #[error("Validation error: {message}")]
    Join { message: String },

    /// A form stood in a context the language does not admit it in.
    #[leaf("context", class = Syntax, summary = "A form stood in a context it is not admitted in.")]
    #[error("Validation error: {message}")]
    Context { message: String },

    /// A form the language admits that this release does not support on
    /// the road it took.
    #[leaf("unsupported", class = Syntax, summary = "A form this release does not support.")]
    #[error("Validation error: {message}")]
    Unsupported { message: String },

    /// An aggregate call stood beside plain columns with no grouping
    /// stated, and the language does not aggregate implicitly.
    #[leaf("implicit_aggregation", class = Syntax, summary = "An aggregate stood beside ungrouped columns.")]
    #[error("Validation error: {message}")]
    ImplicitAggregation { message: String },

    /// An ordinary aggregate stood where the value it reduces is already one
    /// row of a reduction: inside another aggregate's argument, or inside the
    /// row a collecting record or tuple gathers. A reduction of a reduction
    /// needs a relation boundary between the two, written as a stage.
    #[leaf("nested_reduction", class = Syntax, summary = "An aggregate stood inside another reduction without a relation boundary.")]
    #[error("Validation error: {message}")]
    NestedReduction { message: String },

    /// A metadata group yields one record per group; a per-row reading of
    /// it has no meaning.
    #[leaf("metadata_per_row", class = Syntax, summary = "A metadata group was read per row.")]
    #[error("Validation error: {message}")]
    MetadataPerRow { message: String },

    /// An argumentative functor passed as a higher-order parameter must
    /// match the arity the parameter declares.
    #[leaf("ho_param/argumentative_functor/arity", class = Syntax, summary = "An argumentative functor parameter's arity mismatches.")]
    #[error("Validation error: {message}")]
    HoParamArgumentativeFunctorArity { message: String },
}

/// `semantic/limitation/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Limitation))]
pub enum Limitation {
    /// The form is admitted by the language and not yet implemented on this
    /// road. The message names the form.
    #[leaf("not_implemented", class = Syntax, summary = "A form this release does not implement.")]
    #[error("Not implemented: {message}")]
    NotImplemented { message: String },
}

/// `semantic/identifier/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Identifier))]
pub enum Identifier {
    /// Exact `_` is reserved deixis: it points at the one unnamed pipe stage
    /// in reference position and disregards a slot in binding position. It
    /// is never an authored identifier or alias, and stropping is spelling —
    /// `` `_` `` does not release the reservation. Longer underscore
    /// spellings such as `__` and `_fn` are ordinary names.
    #[leaf("deixis", class = Syntax, summary = "Exact `_` written as an authored name.")]
    #[error("Validation error: {message}")]
    Deixis { message: String },
}

/// `semantic/anon/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Anon))]
pub enum Anon {
    /// A sparse row supplies more or fewer values than the header names.
    #[leaf("sparse_arity", class = Syntax, summary = "A sparse row's width disagrees with the header.")]
    #[error("Validation error: {message}")]
    SparseArity { message: String },

    /// A sparse row fills one position twice.
    #[leaf("sparse_duplicate", class = Syntax, summary = "A sparse row fills a position twice.")]
    #[error("Validation error: {message}")]
    SparseDuplicate { message: String },

    /// A sparse fill names a position the header does not declare.
    #[leaf("sparse_fill_position", class = Syntax, summary = "A sparse fill names an undeclared position.")]
    #[error("Validation error: {message}")]
    SparseFillPosition { message: String },

    /// A sparse form needs a header naming its positions.
    #[leaf("sparse_header", class = Syntax, summary = "A sparse form has no header.")]
    #[error("Validation error: {message}")]
    SparseHeader { message: String },

    /// A sparse form's suffix is not one the sparse spelling admits.
    #[leaf("sparse_suffix", class = Syntax, summary = "A sparse form's suffix is ill-formed.")]
    #[error("Validation error: {message}")]
    SparseSuffix { message: String },
}

/// `semantic/cfe/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Cfe))]
pub enum Cfe {
    /// A query-scoped value function was called with the wrong number of
    /// arguments.
    #[leaf("arity", class = Syntax, summary = "A value function was called with the wrong arity.")]
    #[error("Validation error: {message}")]
    Arity { message: String },

    /// A code argument (a target-language expression) stood where a value
    /// function takes a value.
    #[leaf("code_argument", class = Syntax, summary = "A code argument stood where a value is taken.")]
    #[error("Validation error: {message}")]
    CodeArgument { message: String },

    /// A cover applies a value function per column; the function's arity
    /// and the cover's disagree.
    #[leaf("cover_arity", class = Syntax, summary = "A cover's function arity disagrees with the cover.")]
    #[error("Validation error: {message}")]
    CoverArity { message: String },

    /// A value function's body reads a name its formals do not declare.
    #[leaf("formals/undeclared", class = Syntax, summary = "A value function reads an undeclared formal.")]
    #[error("Validation error: {message}")]
    FormalsUndeclared { message: String },

    /// A lambda was applied with the wrong number of arguments.
    #[leaf("lambda_arity", class = Syntax, summary = "A lambda was applied with the wrong arity.")]
    #[error("Validation error: {message}")]
    LambdaArity { message: String },

    /// A value function declares one parameter name twice.
    #[leaf("parameter/duplicate", class = Syntax, summary = "A value function declares a parameter twice.")]
    #[error("Validation error: {message}")]
    ParameterDuplicate { message: String },

    /// A query-scoped value function reaches itself; value functions do not
    /// recurse.
    #[leaf("recursion", class = Syntax, summary = "A value function recurses.")]
    #[error("Validation error: {message}")]
    Recursion { message: String },
}

/// `semantic/compression/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Compression))]
pub enum Compression {
    /// A compression's degree is not one the operator admits.
    #[leaf("degree", class = Syntax, summary = "A compression's degree is not admitted.")]
    #[error("Validation error: {message}")]
    Degree { message: String },

    /// A compression's base relation has no source to compress from.
    #[leaf("sourceless_base", class = Syntax, summary = "A compression's base has no source.")]
    #[error("Validation error: {message}")]
    SourcelessBase { message: String },
}

/// `semantic/cte/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Cte))]
pub enum Cte {
    /// A query-scoped binding's preamble nested inside another binding's;
    /// bindings stand one after another in the block.
    #[leaf("nested_preamble", class = Syntax, summary = "A binding's preamble nested inside another.")]
    #[error("Validation error: {message}")]
    NestedPreamble { message: String },
}

/// `semantic/expansion/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Expansion))]
pub enum Expansion {
    /// A definition's expansion left an interior slot that nothing at the
    /// use site fills.
    #[leaf("interior_slot", class = Syntax, summary = "An expansion left an unfilled interior slot.")]
    #[error("Validation error: {message}")]
    InteriorSlot { message: String },

    /// A definition's interior was shaped at expansion in a way its
    /// declaration does not admit.
    #[leaf("shaping_interior", class = Syntax, summary = "An expansion shaped an interior it may not.")]
    #[error("Validation error: {message}")]
    ShapingInterior { message: String },
}

/// `semantic/fact_function/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::FactFunction))]
pub enum FactFunction {
    /// A fact function names one column twice across its inputs and
    /// outputs.
    #[leaf("duplicate_name", class = Syntax, summary = "A fact function names a column twice.")]
    #[error("Validation error: {message}")]
    DuplicateName { message: String },

    /// A fact function's output arm reads none of its inputs, so the
    /// function is constant over an unbounded domain.
    #[leaf("output_reads_no_input", class = Syntax, summary = "A fact function's output reads no input.")]
    #[error("Validation error: {message}")]
    OutputReadsNoInput { message: String },

    /// A fact function's rows disagree with its declared width.
    #[leaf("width", class = Syntax, summary = "A fact function's rows disagree with its width.")]
    #[error("Validation error: {message}")]
    Width { message: String },
}

/// `semantic/ho/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Ho))]
pub enum HoDefinition {
    /// A higher-order view's scalar parameter is spliced into the body as a
    /// value, not a column. When its name equals a column the body would
    /// otherwise resolve to (e.g. `g(age)(*) :- users(*), age > 40` where
    /// users has an age column), the substitution silently CAPTURES the
    /// column: constraints on it tautologize (`age > 40` becomes `50 > 40`)
    /// and the column drops from the output — both silent. Refused loudly
    /// at expansion (call time), where body relations carry real schemas,
    /// so both concretely-named bodies and glob-param bodies (T(*)) are
    /// caught. Remedy: rename the parameter so it no longer shadows the
    /// column.
    #[leaf("param_shadows_column", class = Syntax, summary = "A scalar parameter name collides with a body column.")]
    #[error("Validation error: {message}")]
    ParamShadowsColumn { message: String },

    /// A bare scalar actual resolves to more than one caller occurrence, so
    /// the call cannot say which value it supplies.
    #[leaf("actual/ambiguous_occurrence", class = Syntax, summary = "A scalar actual names more than one caller occurrence.")]
    #[error("Validation error: {message}")]
    ActualAmbiguousOccurrence { message: String },
}

/// `semantic/interior/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Interior))]
pub enum Interior {
    /// RULED (this release): under non-equality correlation (<=, <, ...)
    /// each outer row sees a DIFFERENT candidate population, and the
    /// pre-ranked lowering would rank the wrong one — so the form refuses
    /// rather than answer arbitrarily. Spell the ranking explicitly with a
    /// post-join row_number window. A lateral per-outer-row lowering is a
    /// possible future release.
    #[leaf("topn/noneq_correlation", class = Syntax, summary = "Interior top-N requires equality correlation.")]
    #[error("Validation error: {message}")]
    TopnNoneqCorrelation { message: String },

    /// The interior's top-N partition cannot be proved from its
    /// correlation, so the pre-ranked lowering has no partition key.
    #[leaf("topn/unprovable_partition", class = Syntax, summary = "Interior top-N has no provable partition.")]
    #[error("Validation error: {message}")]
    TopnUnprovablePartition { message: String },

    /// A correlated interior is realized by evaluating its correlation at
    /// the enclosing join, which must still read the interior column the
    /// correlation names. An operation inside the interior that cannot keep
    /// that column readable without changing what the interior means — a
    /// set operation, a witness, a grouping that does not group by it —
    /// refuses rather than dropping the obligation.
    #[leaf("correlation/support", class = Syntax, summary = "A correlated interior cannot carry its correlation through this operation.")]
    #[error("Validation error: {message}")]
    CorrelationSupport { message: String },

    /// A position a hoisted interior publishes FROM THE ENCLOSING ROW — a
    /// projection item that reads the row the interior is correlated to —
    /// is evaluated at the enclosing join, the one place that row is
    /// readable. Nothing inside the interior can read the position: a
    /// restriction, a grouping key, a later item or an ordering over it
    /// would need a value that is not computed until the boundary is
    /// crossed, and a set operation, witness or reduction over it would
    /// change what the position means.
    #[leaf("correlation/enclosing_position", class = Syntax, summary = "A position computed from the enclosing row cannot be read inside its interior.")]
    #[error("Validation error: {message}")]
    EnclosingPosition { message: String },

    /// A correlated interior's row bound is realized per outer row as ONE
    /// rank interval — the rows the bound skips and the rows it keeps. A
    /// bound that is not one interval (a second bound standing on the same
    /// run, or an interval no rank can spell) refuses rather than keeping
    /// the cap and dropping the rest.
    #[leaf("topn/bound_interval", class = Syntax, summary = "A correlated interior's row bound is not one rank interval.")]
    #[error("Validation error: {message}")]
    BoundInterval { message: String },
}

/// `semantic/join/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Join))]
pub enum Join {
    /// A `using` join names its shared columns; an extra stated condition
    /// beside them is a second correspondence.
    #[leaf("using/extra_condition", class = Syntax, summary = "A `using` join carried an extra condition.")]
    #[error("Validation error: {message}")]
    UsingExtraCondition { message: String },

    /// A `using` join's shared columns are the condition; a stated
    /// condition in its place is not admitted.
    #[leaf("using/stated_condition", class = Syntax, summary = "A `using` join stated a condition.")]
    #[error("Validation error: {message}")]
    UsingStatedCondition { message: String },
}

/// `semantic/landing/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Landing))]
pub enum Landing {
    /// A call both binds the pipe with a binder and leaves a hole for it;
    /// one landing, one spelling.
    #[leaf("binder_and_hole", class = Syntax, summary = "A landing was both bound and left as a hole.")]
    #[error("Validation error: {message}")]
    BinderAndHole { message: String },

    /// A piped relation reached a call that discards it: nothing lands it,
    /// so the pipe's input would be silently dropped.
    #[leaf("discarded", class = Syntax, summary = "A piped relation was discarded.")]
    #[error("Validation error: {message}")]
    Discarded { message: String },

    /// A call left two holes for one pipe.
    #[leaf("two_holes", class = Syntax, summary = "Two holes for one pipe.")]
    #[error("Validation error: {message}")]
    TwoHoles { message: String },
}

/// `semantic/mention/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Mention))]
pub enum Mention {
    /// A delimited mention's interior is an identifier the mention grammar
    /// does not admit.
    #[leaf("identifier_interior", class = Syntax, summary = "A mention's interior is not an admitted identifier.")]
    #[error("Validation error: {message}")]
    IdentifierInterior { message: String },

    /// A term is the interior of a mention — the committed extent is table
    /// functors only: a single relation-access term such as people(*),
    /// people(, age >= 30), or orders(id, _, total). Namespace paths,
    /// function terms, pipelines, and joins are not terms; new term kinds
    /// are admitted by ruling, never by drift.
    #[leaf("term/not_a_term", class = Syntax, summary = "The mention's interior is not an admitted term.")]
    #[error("Validation error: {message}")]
    TermNotATerm { message: String },

    /// The term parses but the format engine takes no position on part of
    /// it — and unformatted bytes never become a match key or a stored
    /// spelling, because the canonical form is both.
    #[leaf("term/unformattable", class = Syntax, summary = "The canonicalizer cannot emit this term.")]
    #[error("Validation error: {message}")]
    TermUnformattable { message: String },
}

/// `semantic/mode/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Mode))]
pub enum Mode {
    /// A declared mode's arity disagrees with the fact function's.
    #[leaf("arity", class = Syntax, summary = "A mode's arity disagrees with its function's.")]
    #[error("Validation error: {message}")]
    Arity { message: String },

    /// A mode application's degree is not one the mode admits.
    #[leaf("degree", class = Syntax, summary = "A mode application's degree is not admitted.")]
    #[error("Validation error: {message}")]
    Degree { message: String },

    /// A mode was applied that the fact function does not declare.
    #[leaf("undeclared", class = Syntax, summary = "An undeclared mode was applied.")]
    #[error("Validation error: {message}")]
    Undeclared { message: String },

    /// A mode names an output the fact function does not declare.
    #[leaf("unknown_output", class = Syntax, summary = "A mode names an unknown output.")]
    #[error("Validation error: {message}")]
    UnknownOutput { message: String },
}

/// `semantic/narrowing/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Narrowing))]
pub enum Narrowing {
    /// A narrowing member is not a shape the narrowing grammar admits.
    #[leaf("member", class = Syntax, summary = "A narrowing member is ill-shaped.")]
    #[error("Validation error: {message}")]
    Member { message: String },

    /// Narrowing (.col{...}) iterates a SEQUENCE — every row of this column
    /// is a single object literal, a record, not a sequence of records.
    /// Path into the record instead ((col:{.field})), or spell the
    /// one-element sequence ([{...}]). Data-borne non-arrays the compiler
    /// cannot see contribute zero rows at runtime (JSON-SUBSTRATE.md).
    #[leaf("object_literal", class = Syntax, summary = "Narrowing a column whose every row is a single object.")]
    #[error("Validation error: {message}")]
    ObjectLiteral { message: String },
}

/// `semantic/set/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Set))]
pub enum Set {
    /// The arms of one set expression correlate in different modes.
    #[leaf("correlation/mixed_modes", class = Syntax, summary = "A set expression's arms correlate in different modes.")]
    #[error("Validation error: {message}")]
    CorrelationMixedModes { message: String },

    /// A correlation operator stood in a set arm that admits none.
    #[leaf("correlation/operator", class = Syntax, summary = "A correlation operator in a set arm.")]
    #[error("Validation error: {message}")]
    CorrelationOperator { message: String },

    /// A correlation stood in a set-arm position it is not admitted in.
    #[leaf("correlation/position", class = Syntax, summary = "A correlation in an inadmissible set position.")]
    #[error("Validation error: {message}")]
    CorrelationPosition { message: String },

    /// A correlated set arm must be named so the correlation can address
    /// it.
    #[leaf("correlation/unnamed_arm", class = Syntax, summary = "A correlated set arm is unnamed.")]
    #[error("Validation error: {message}")]
    CorrelationUnnamedArm { message: String },
}

/// `semantic/setop/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Setop))]
pub enum Setop {
    /// A set operator's correlation could be read against more than one
    /// operand.
    #[leaf("correlation/ambiguous", class = Syntax, summary = "A set operator's correlation is ambiguous.")]
    #[error("Validation error: {message}")]
    CorrelationAmbiguous { message: String },

    /// More than one column of one operand corresponds to a column of the
    /// other, so the corresponding operation cannot align them.
    #[leaf("correspondence/ambiguous", class = Syntax, summary = "More than one column corresponds.")]
    #[error("Validation error: {message}")]
    CorrespondenceAmbiguous { message: String },

    /// The minimum-multiplicity gate refuses a correlation operator inside
    /// a set operation whose multiplicity it cannot bound.
    #[leaf("min_multiplicity/correlation_operator", class = Syntax, summary = "A correlation operator under the minimum-multiplicity gate.")]
    #[error("Validation error: {message}")]
    MinMultiplicityCorrelationOperator { message: String },
}

/// `semantic/set_operation/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::SetOperation))]
pub enum SetOperation {
    /// A positional set operation's operands publish different numbers of
    /// columns.
    #[leaf("column_count_mismatch", class = Syntax, summary = "Set-operation operands differ in column count.")]
    #[error("Validation error: {message}")]
    ColumnCountMismatch { message: String },

    /// A name-aligned set operation's operands publish different names.
    #[leaf("column_name_mismatch", class = Syntax, summary = "Set-operation operands differ in column names.")]
    #[error("Validation error: {message}")]
    ColumnNameMismatch { message: String },
}

/// `semantic/transform/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Transform))]
pub enum Transform {
    /// A membership test's columns cannot be lowered as written.
    #[leaf("membership/columns", class = Syntax, summary = "A membership test's columns have no lowering.")]
    #[error("Validation error: {message}")]
    MembershipColumns { message: String },
}

/// `semantic/using/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Using))]
pub enum Using {
    /// `using all` joins on every shared column name; a name that appears
    /// twice on the left side is ambiguous.
    #[leaf("all/ambiguous-left", class = Syntax, summary = "A shared column is ambiguous on the left.")]
    #[error("Validation error: {message}")]
    AllAmbiguousLeft { message: String },

    /// A shared column name appears twice on the right side.
    #[leaf("all/ambiguous-right", class = Syntax, summary = "A shared column is ambiguous on the right.")]
    #[error("Validation error: {message}")]
    AllAmbiguousRight { message: String },

    /// `using all` found no column name both sides share.
    #[leaf("all/no-shared-columns", class = Syntax, summary = "`using all` found no shared columns.")]
    #[error("Validation error: {message}")]
    AllNoSharedColumns { message: String },
}

/// `semantic/window/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Window))]
pub enum Window {
    /// A windowed function was called without a window specification.
    #[leaf("needs_window", class = Syntax, summary = "A windowed function was called without a window.")]
    #[error("Validation error: {message}")]
    NeedsWindow { message: String },

    /// A window specification was attached to a function that is not a
    /// window function.
    #[leaf("not_a_window", class = Syntax, summary = "A window was attached to a non-window function.")]
    #[error("Validation error: {message}")]
    NotAWindow { message: String },
}
