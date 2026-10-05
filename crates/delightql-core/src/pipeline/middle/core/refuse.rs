// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The refusal identities the core decides. Each function names the law's
//! refusal; the value is the product's diagnostic, so a refusal leaves the
//! middle under the same identity the law gives it.

use delightql_types::diagnostic::{
    Constraint, Directive, Dml, DmlMarker, DmlRoles, DmlShape, DmlSource, Effect, EffectBody, EffectDdl, EffectMain,
    EffectRule, Ho, Interior, Internal, Limitation, Narrowing, Operational, Parse, Recursion, Resolution, Semantic,
    SetOperation, Window,
};
use crate::pipeline::middle::facade::SqlIdentifier as Name;
use delightql_types::DelightQLError;

pub(crate) type Refusal = DelightQLError;

/// A qualified anonymous-table header that names no position of the run
/// the table joins: a header reuses a position and cannot create its owner.
pub(crate) fn header_qualifier(spelled: &str) -> Refusal {
    delightql_types::diagnostic::AnonBinding::Qualifier {
        message: format!("anonymous-table header '{spelled}' names no column of the run the table joins"),
    }
    .into()
}

/// One occurrence standing both as an anonymous-table header and in its
/// data rows: the header's constraint on that position would be vacuous.
pub(crate) fn header_row_lvar(spelled: &str) -> Refusal {
    delightql_types::diagnostic::AnonBinding::HeaderRowLvar {
        message: format!("lvar '{spelled}' appears both as a header and in the data rows of the same anonymous table"),
    }
    .into()
}

/// An anonymous witness whose header introduces a column: an existence
/// marker tests membership and binds nothing.
pub(crate) fn witness_shape(spelled: &str) -> Refusal {
    delightql_types::diagnostic::AnonBinding::WitnessShape {
        message: format!(
            "the witness header '{spelled}' names no column the rows to its left publish: an existence marker \
             introduces no column"
        ),
    }
    .into()
}

/// A named column no live scope answers to.
pub(crate) fn column(column: &str, context: &str) -> Refusal {
    Resolution::Column {
        column: column.to_string(),
        context: context.to_string(),
    }
    .into()
}

/// `_` where no unnamed pipe output is visible (lvars-law `_` IS DEIXIS:
/// zero candidates refuse).
pub(crate) fn no_unnamed_pipe(spelled: &str) -> Refusal {
    delightql_types::diagnostic::Pipe::NoUnnamedPipe {
        message: format!("`{spelled}`: there is no unnamed pipe output here for `_` to select"),
    }
    .into()
}

/// `_` where two or more unnamed pipe outputs are visible (lvars-law `_` IS
/// DEIXIS: one spelling cannot stand for two relations).
pub(crate) fn two_unnamed_pipes(spelled: &str) -> Refusal {
    delightql_types::diagnostic::Pipe::TwoUnnamedPipes {
        message: format!("`{spelled}`: several unnamed pipe outputs are in scope and `_` names none of them; name one with `as`"),
    }
    .into()
}

/// `.*` at an interior's head sharing no name with the enclosing rows: a
/// correlation of nothing is no step it can perform.
pub(crate) fn using_no_shared() -> Refusal {
    delightql_types::diagnostic::Using::AllNoSharedColumns {
        message: "`.*` at an interior's head finds no name the enclosing rows share with it".to_string(),
    }
    .into()
}

/// A qualified reference whose qualifier no live scope answers to; the
/// message names the scopes that are live.
pub(crate) fn not_a_scope(qualifier: &str, spelled: &str, live: &[String]) -> Refusal {
    let seen = match live {
        [] => "(nothing in scope answers a qualifier)".to_string(),
        _ => format!("(in scope: {})", live.join(", ")),
    };
    column(&format!("{spelled} — '{qualifier}' is not a scope here {seen}"), "no live scope has this qualifier")
}

/// A misused `cast:`: the law's refusal names what the second argument
/// must be.
pub(crate) fn cast(message: &str) -> Refusal {
    Semantic::Cast {
        message: message.to_string(),
    }
    .into()
}

/// A window attached to a call that is not a window function.
pub(crate) fn not_a_window(name: &str, what: &str) -> Refusal {
    Window::NotAWindow {
        message: format!(
            "the window rides the window function itself, and '{name}' is {what} — it computes \
             per row and takes no window"
        ),
    }
    .into()
}

/// A guard on a window function that is not an aggregate: a guard filters
/// the contributions an aggregate reads, and a ranking or offset function
/// reads none.
pub(crate) fn guard_on_window_function(name: &str) -> Refusal {
    Window::Guard {
        message: format!(
            "a guard filters the rows an aggregate reads, and '{name}' is a window function that aggregates \
             nothing — filter the relation before the window, or test the condition in a case around the call"
        ),
    }
    .into()
}

/// A whole-operand `*` handed to an aggregate the target does not let
/// count rows.
pub(crate) fn star_argument(name: &str, dialect: &str) -> Refusal {
    Constraint::Context {
        message: format!(
            "`{name}:` is an aggregate on {dialect} that takes no whole-operand `*`: the star stands \
             for the row itself, and only a call whose catalog row admits it (as `count:(*)` does) \
             counts rows — name the value it reduces (`{name}:(column)`)"
        ),
    }
    .into()
}

/// An application whose actuals do not fill its declared parameter row.
pub(crate) fn incomplete_application(message: &str) -> Refusal {
    Ho::IncompleteApplication {
        message: message.to_string(),
    }
    .into()
}

/// An argumentative access standing at a relation formal (top-grammar
/// FN.11: it is not a relation-valued actual; its names are logical
/// binders).
pub(crate) fn relation_actual_form(parameter: &str, entity: &str) -> Refusal {
    Ho::RelationActualForm {
        message: format!(
            "parameter '{parameter}' of '{entity}' takes a closed relation value, and an argumentative access is \
             not one: its names are logical binders. Pass the whole relation (`name(*)`), an anonymous relation, or \
             an interior over its own source, or bind the relation with `:` and pass it whole"
        ),
    }
    .into()
}

/// A written relation actual that reads the calling row.
pub(crate) fn relation_actual_capture(read: Option<&str>) -> Refusal {
    Ho::RelationActualCapture {
        message: match read {
            Some(name) => format!(
                "a relation actual is a closed relation value: its interior may not read `{name}` from \
                 the calling row"
            ),
            None => "a relation actual is a closed relation value: its interior may not read the calling \
                     row"
                .to_string(),
        },
    }
    .into()
}

/// A value standing where a rule formal takes a rule value.
pub(crate) fn rule_value_form(parameter: &str, entity: &str) -> Refusal {
    Ho::RuleValueForm {
        message: format!("parameter '{parameter}' of '{entity}' requires a closed residual rule value"),
    }
    .into()
}

/// A designator written with an access group after its configured prefix
/// (`rule(2)(*)`) at a rule-valued parameter: a completed application, not
/// the configured designator `rule(2)` (FN.48).
pub(crate) fn rule_value_applied(rule: &str) -> Refusal {
    Ho::RuleValueForm {
        message: format!(
            "'{rule}' is written with an access group after its configured values, which applies it: a \
             rule-valued parameter receives the designator itself, written `{rule}(…)` without the access group"
        ),
    }
    .into()
}

/// A configured value naming a column a sibling argument of the call
/// publishes (FN.48): it closes over the relation standing at its
/// designator — the landed relation, or a member written to its left in the
/// same run — and never over another argument of the call.
pub(crate) fn residual_capture(entity: &str, column: &str) -> Refusal {
    Ho::ResidualCapture {
        message: format!(
            "a configured value reads '{column}', a column of another argument of '{entity}': a configured value is \
             read over the relation standing at its designator (the relation landed at the call, or a member to its \
             left in the same run), never over a sibling argument"
        ),
    }
    .into()
}

/// The column a refusal says no live scope answers, if it says that.
pub(crate) fn unanswered_column(refusal: &Refusal) -> Option<&str> {
    match refusal {
        DelightQLError::Semantic(Semantic::Resolution(Resolution::Column { column, .. })) => Some(column),
        _ => None,
    }
}

/// A relation that names no family standing at a rule-valued parameter
/// (FN.48: a rule-valued position designates a family; an anonymous
/// relation designates none).
pub(crate) fn rule_value_form_of_relation() -> Refusal {
    Ho::RuleValueForm {
        message: "a rule-valued parameter receives a relation that names no family: it requires a closed residual \
                  rule value, a family designated by name (`rule(*)`, `rule(2)`)"
            .to_string(),
    }
    .into()
}

/// Ground values lifted into a relation formal that scalar formals follow,
/// with no `&` to mark where the lifted rows end.
pub(crate) fn lifted_boundary(entity: &str, parameter: &str) -> Refusal {
    Ho::LiftedBoundary {
        message: format!(
            "ambiguous lifted-relation boundary in '{entity}': inline rows for parameter '{parameter}' \
             are followed by scalar parameter(s), and the split cannot be guessed"
        ),
    }
    .into()
}

/// A literal actual at a position every clause grounds, matching none of
/// their ground members (grounding-and-mention-law A PROVABLE MISS IS AN
/// ERROR).
pub(crate) fn provable_miss(entity: &str, position: usize, actual: &str, declared: &[String]) -> Refusal {
    delightql_types::diagnostic::Grounding::HeadProvableMiss {
        message: format!(
            "no clause of '{entity}' grounds on '{actual}' at parameter {position}: the declaration alone proves this \
             call empty, so it refuses instead of answering no rows. The clauses ground parameter {position} on {}. \
             A value the call does not write there as a literal (a column, a forwarded formal) misses to empty",
            declared.join(", "),
            position = position + 1
        ),
    }
    .into()
}

/// A free data name of a declaration no grounding bound.
pub(crate) fn data_hole(name: &str, world: &str) -> Refusal {
    delightql_types::diagnostic::Grounding::DataHoleUnbound {
        message: format!(
            "'{name}' is a free data name of '{world}', and no ground! has bound that world's data holes to \
             a data world. A consulted body reads its own definitions and the data world an explicit \
             grounding published — never the caller's tables, CTEs, or session database ambiently"
        ),
    }
    .into()
}

/// A piped relation's landing at a call that has no lawful place for it.
pub(crate) fn pipe_landing(message: &str) -> Refusal {
    Ho::PipeLanding {
        message: message.to_string(),
    }
    .into()
}

/// A call of a DQL relation that declares no parameter row.
pub(crate) fn not_parameterized(name: &str) -> Refusal {
    Resolution::CallableUnknown {
        message: format!(
            "'{name}' names a DQL relation — not a parameterized relation. Name selects the identity \
             before its kind is judged, so the call refuses here rather than reaching another '{name}' \
             or the target engine; to call the engine's own table function write \
             sys::target.{name}(…)(*)"
        ),
    }
    .into()
}

/// A value definition applied to another number of arguments than it
/// declares.
pub(crate) fn value_arity(name: &str, declared: usize, supplied: usize) -> Refusal {
    delightql_types::diagnostic::Cfe::Arity {
        message: format!(
            "'{name}' expects {declared} argument{}, got {supplied}",
            if declared == 1 { "" } else { "s" }
        ),
    }
    .into()
}

/// A relational call whose callee no DQL entity answers to.
pub(crate) fn callable_unknown(name: &str) -> Refusal {
    Resolution::CallableUnknown {
        message: format!(
            "no DQL callable '{name}' is defined or selected here; to call the target engine's own \
             table function write sys::target.{name}(…)(*)"
        ),
    }
    .into()
}

/// A callee whose name selects a DQL definition that is no callable of the
/// kind its position asks for: the name selected that definition, so the
/// call never falls through to the target (THE TARGET SURFACE IS OPEN).
pub(crate) fn callable_of_another_kind(name: &str, kind: &str) -> Refusal {
    Resolution::CallableUnknown {
        message: format!(
            "'{name}' selects a DQL {kind}, which is not a callable of the kind this position calls; a call \
             reaches the target only where no DQL definition answers its name (to call the target's own \
             '{name}', write sys::target.{name})"
        ),
    }
    .into()
}

/// A bound, an offset or a column ordinal whose scalar formal stands for no
/// integer the position takes (domain-expressions FN.40): the actual is not
/// a whole-number literal, or the integer is outside the position's range.
pub(crate) fn integer_value(message: String) -> Refusal {
    Semantic::LimitValue { message }.into()
}

/// A column ordinal whose integer no position can have, as a written
/// ordinal of that size is refused.
pub(crate) fn ordinal_range(n: i64) -> Refusal {
    Constraint::General {
        message: format!("column ordinal |{n}| is out of range"),
    }
    .into()
}

/// A bound, an offset or a column ordinal naming a scalar formal, met where
/// its definition is declared: no use supplies the formal there, so what
/// the position stands for is left to each use, as every judgment a
/// declaration makes over stand-ins leaves what it cannot decide.
pub(crate) fn integer_at_use() -> Refusal {
    Operational::Uncovered {
        message: "what a scalar formal stands for in a bound or an ordinal is decided at each use of its \
                  definition; no use supplies it where the definition is declared"
            .to_string(),
    }
    .into()
}

/// A relation read whose name selects a DQL definition with no relation
/// face: a value function is called, never read.
pub(crate) fn function_read_as_relation(name: &str, kind: &str) -> Refusal {
    Resolution::General {
        message: format!(
            "'{name}' selects a DQL {kind}, which has no relation to read; call it as {name}:(…) in value position"
        ),
    }
    .into()
}

/// A qualified value callee its namespace does not hold: a qualified call
/// selects only there, and never falls back to the target.
pub(crate) fn qualified_callee_unknown(spelled: &str) -> Refusal {
    Resolution::CallableUnknown {
        message: format!(
            "no DQL function '{spelled}' is defined in that namespace; a qualified call selects only there \
             (the target's own function is written sys::target.name:(…))"
        ),
    }
    .into()
}

/// An engine reference whose relation the engine's own catalog does not
/// hold: there is no heading to read its names by.
pub(crate) fn no_engine_heading(spelled: &str) -> Refusal {
    Resolution::Schema {
        message: format!("the target engine's catalog holds no relation '{spelled}', so it has no heading to read"),
    }
    .into()
}

/// An edge used where its term names another relation than the one its
/// declaring body reads under that spelling.
pub(crate) fn edge_term_world(term: &str, context: &str) -> Refusal {
    delightql_types::diagnostic::Er::TermWorld {
        message: format!(
            "'{term}' names one relation where this '::{context}' edge is used and another where the edge is \
             declared; name the declaring namespace's relation explicitly"
        ),
    }
    .into()
}

/// A named relation no scope or catalog answers to.
pub(crate) fn table(table: &str) -> Refusal {
    Resolution::Table {
        table: table.to_string(),
        context: String::new(),
    }
    .into()
}

/// COMPILE PURITY: an inspection past the front end runs nothing, so a
/// relation of the inspected text that the runtime executes refuses before
/// anything compiles.
pub(crate) fn compile_purity(stage: &str, served: &str) -> Refusal {
    Effect::CompilePurity {
        message: format!(
            "sys::execution.compile is pure: compiling to stage '{stage}' would execute '{served}' — inspection \
             must never mutate the namespace, database, filesystem, output, or session. Compile to 'cst' or \
             'ast-unresolved' to inspect this source, or run it as a query to execute it."
        ),
    }
    .into()
}

/// `@` in a table-level companion cell: no column is the cell's own
/// (ddl-grammar FN.2).
pub(crate) fn table_level_column_self() -> Refusal {
    Constraint::General {
        message: "A table-level DDL expression cannot use the value placeholder".to_string(),
    }
    .into()
}

/// A table-level key cell that names no column: a key of the carrying
/// column is spelled on that column's own row (ddl-grammar FN.1).
pub(crate) fn table_key_without_columns(primary: bool) -> Refusal {
    delightql_types::diagnostic::Manifest::TableConstraintColumns {
        message: if primary {
            "A table-level primary key must name at least one column".to_string()
        } else {
            "A table-level unique constraint must name at least one column".to_string()
        },
    }
    .into()
}

/// A foreign key in a table-level cell: its one column list cannot name
/// both the referencing and the referenced columns; it is written on the
/// referencing column's own row.
pub(crate) fn table_foreign_key() -> Refusal {
    delightql_types::diagnostic::Manifest::TableForeignKey {
        message: "A foreign key in a table-level constraint cell names one column list, which cannot be both the \
                  referencing and the referenced columns: write it on the referencing column's own row"
            .to_string(),
    }
    .into()
}

/// A bare name that several positions lost by collision.
pub(crate) fn ambiguous_column(name: &impl std::fmt::Display) -> Refusal {
    Constraint::General {
        message: format!(
            "Column '{name}' is ambiguous: several positions publish it; address one through \
             its qualifier or by position"
        ),
    }
    .into()
}

/// A transform target several positions publish.
pub(crate) fn ambiguous_transform_target(name: &Name) -> Refusal {
    Resolution::Ambiguous {
        message: format!("Ambiguous transform target '{name}'"),
    }
    .into()
}

/// One header may not publish two columns under one name; stropping opens
/// no second name.
pub(crate) fn duplicate_header_name(name: &impl std::fmt::Display) -> Refusal {
    Constraint::General {
        message: format!(
            "Duplicate column '{name}' in the header of an anonymous relation: programmer-authored \
             names must be unique, and stropping does not make a second name. Name one of the \
             columns differently"
        ),
    }
    .into()
}

/// A clause ending in a signed witness over its receipt (THE ENDING LAW, THE
/// RECEIPT CORE): a witnessed row is a ledger row, not a receipt.
pub(crate) fn effect_ending_witnessed(rule: &str) -> Refusal {
    EffectRule::Ending {
        message: format!(
            "effect rule '{rule}': its body ends in a signed witness (`+-`), and a witnessed row is not a \
             receipt — a step that did nothing yields a row whose `success` is NULL where a receipt would be \
             absent. Package the witnessed row: `|> returning!(*)`"
        ),
    }
    .into()
}

/// A correlated interior's population passed through a definition instance
/// (register L4).
pub(crate) fn correlation_through_instance() -> Refusal {
    Interior::CorrelationSupport {
        message: "a correlated interior cannot carry its population through a definition instance (a known \
                  limitation): apply the definition before correlating — bind `o(*) |> f(*)` to a name and \
                  correlate that name"
            .to_string(),
    }
    .into()
}

/// A declared residual contract offered a family whose heading is open
/// (FN.48: a declared final group is exact).
pub(crate) fn residual_contract_open(rule: &str) -> Refusal {
    Ho::ResidualContract {
        message: format!(
            "the residual of '{rule}' publishes an open heading, and the rule-valued parameter it stands at \
             declares an exact one: a declared final group is exact and nothing is trimmed to fit. Declare the \
             parameter open (`P(... T(*))(*)`) and project after"
        ),
    }
    .into()
}

/// A function declaring an empty capture `..{}` (FN.38).
pub(crate) fn empty_capture(function: &str) -> Refusal {
    Constraint::General {
        message: format!(
            "'{function}' declares an empty capture `..{{}}`, which captures nothing: remove the marker \
             ({function}:(…) without `..{{}}`)"
        ),
    }
    .into()
}

/// An implicit capture called with its captured columns written as values
/// (FN.38).
pub(crate) fn implicit_capture_positional(function: &str) -> Refusal {
    Constraint::General {
        message: format!(
            "'{function}' captures its caller's row implicitly (`..`), and an implicit capture has no declared \
             order, so its captured columns cannot be written as values: call it with `..` \
             ({function}:(.., …)), or declare the capture (`..{{…}}`) to supply it positionally"
        ),
    }
    .into()
}

/// A pattern iterating a value the query made as one record (json-substrate
/// law: the knowable half refuses).
pub(crate) fn pattern_object_literal(column: &str) -> Refusal {
    Narrowing::ObjectLiteral {
        message: format!(
            "the pattern iterates '{column}' (`~>`), and every row of '{column}' is one record the query made: \
             pattern into the record instead ({column} ~= {{…}}), or make a one-element sequence ([{{…}}])"
        ),
    }
    .into()
}

/// Two sequences one pattern iterates side by side (top-grammar: ONE
/// ITERATION PER PATTERN LEVEL).
pub(crate) fn side_by_side_iteration() -> Refusal {
    Resolution::Ambiguous {
        message: "two sequences iterated side by side in one pattern admit two readings (every combination, or \
                  pairing by position): iterate them in sequential steps — bind each sequence by key, then \
                  iterate each with its own `~=`"
            .to_string(),
    }
    .into()
}

/// A pivot candidate that is a number or NULL (pipe-operators FN.10).
pub(crate) fn pivot_key_spelling(key: &str, candidate: &str) -> Refusal {
    Constraint::Pivot {
        message: format!(
            "the pivot key '{key}' has the candidate {candidate}, which does not name a column (a column named \
             by a number would be confused with an ordinal, and NULL names nothing): name the columns with a \
             template over the key (`v of :\"{key}{{{key}}}\"`), or compute a text key before the pivot"
        ),
    }
    .into()
}

/// A SELECTOR MUST ADDRESS A COLUMN (addressing-matrix-law): a spread that
/// addresses nothing, or only names lost to a collision.
pub(crate) fn selector_empty(spelling: &str, lost: &[String]) -> Refusal {
    let why = if lost.is_empty() {
        String::new()
    } else {
        format!(
            "; the names it matches ({}) are lost by collision and answer to nothing — name them apart \
             before selecting them",
            lost.join(", ")
        )
    };
    Constraint::SelectorEmpty {
        message: format!(
            "{spelling} addresses no column of its input{why}: a pattern, range or glob that addresses nothing \
             is refused wherever it is written"
        ),
    }
    .into()
}

/// A delegate payload naming a grouping key the grouping position already
/// publishes (DUPLICATE AUTHORED NAMES REFUSE).
pub(crate) fn delegate_key_repeated(name: &impl std::fmt::Display) -> Refusal {
    Constraint::General {
        message: format!(
            "Duplicate column '{name}': the grouping key '{name}' is already published by its grouping \
             position, so naming it in the delegate's payload publishes it twice. Drop it from the payload"
        ),
    }
    .into()
}

/// A covered or renamed column published under the name of a column that
/// rides through (DUPLICATE AUTHORED NAMES REFUSE).
pub(crate) fn riding_name(name: &impl std::fmt::Display) -> Refusal {
    Constraint::General {
        message: format!(
            "Duplicate column '{name}': a column of the stage input rides through under '{name}', so \
             publishing another column under it names '{name}' twice. Give the new column a distinct name \
             (a template such as :\"{{@}}_new\"), or cover the column in place with $(…)"
        ),
    }
    .into()
}

/// A name template with no placeholder (pipe-operators-grammar FN.26).
pub(crate) fn template_without_placeholder(template: &str) -> Refusal {
    Constraint::General {
        message: format!(
            "the name template :\"{template}\" writes no placeholder, so it does not vary with the column it \
             names: write {{@}} (the column's name) or {{#}} (its position), or name one column with the plain \
             form (|> +(f:(a) as name))"
        ),
    }
    .into()
}

/// DUPLICATE AUTHORED NAMES REFUSE; a projection's refusal says it is one.
pub(crate) fn duplicate_name(name: &impl std::fmt::Display, projection: bool) -> Refusal {
    Constraint::General {
        message: format!(
            "Duplicate column '{name}'{}: programmer-authored names must be unique. Rename one with \
             'as' to disambiguate",
            if projection { " in projection" } else { "" }
        ),
    }
    .into()
}

/// A head that names a column its body does not publish.
pub(crate) fn head_name_absent(name: &impl std::fmt::Display) -> Refusal {
    Resolution::Column {
        column: name.to_string(),
        context: "the definition's head names a column its body does not publish".to_string(),
    }
    .into()
}

/// A positional pattern of the wrong width.
pub(crate) fn arity(written: usize, width: usize) -> Refusal {
    Semantic::Arity {
        message: format!(
            "Positional pattern incomplete - the relation has {width} columns but pattern specifies {written} \
             elements"
        ),
    }
    .into()
}

/// A membership candidate row whose width is not the probe's.
pub(crate) fn membership_arity(candidate: usize, probe: usize) -> Refusal {
    Semantic::MembershipArity {
        message: format!("membership candidate has {candidate} value(s) but the probe has {probe}"),
    }
    .into()
}

/// A higher-order view read as a relation, with no parameter row.
pub(crate) fn not_a_relation(name: &str) -> Refusal {
    Semantic::Arity {
        message: format!(
            "'{name}' is a higher-order view, not a relation — supply its relation argument, for example \
             `{name}(source(*))(*)`"
        ),
    }
    .into()
}

/// A positional drill whose slots do not match the interior's width.
pub(crate) fn drill_arity(name: &str, width: usize, written: usize) -> Refusal {
    Constraint::General {
        message: format!(
            "Interior drill-down: the drill of '{name}' names {written} columns; its interior has {width}"
        ),
    }
    .into()
}

/// A drill of a column that carries no interior heading.
pub(crate) fn no_interior(name: &str) -> Refusal {
    Constraint::General {
        message: format!("Interior drill-down: column '{name}' has no known interior heading"),
    }
    .into()
}

/// What a correlated population cannot carry, written inline.
pub(crate) fn correlation_support(what: &str) -> Refusal {
    Interior::CorrelationSupport {
        message: format!(
            "a correlated interior cannot carry its population through {what}: group by the \
             correlated column, window the value over the population, or compute it outside \
             the interior"
        ),
    }
    .into()
}

/// A bound over a population a comparison by another operator selects,
/// written inline.
pub(crate) fn topn_noneq_comparison(op: &str) -> Refusal {
    Interior::TopnNoneqCorrelation {
        message: format!(
            "interior top-N requires equality correlation: '{op}' selects a population no \
             equality names, and a bound over it is not admitted"
        ),
    }
    .into()
}

/// A bound over a population a condition that is no conjunction of
/// equalities selects, written inline; `form` names what it contains.
pub(crate) fn topn_noneq_form(form: &str) -> Refusal {
    Interior::TopnNoneqCorrelation {
        message: format!(
            "interior top-N requires equality correlation, provable as a conjunction of equalities — \
             this correlation contains {form}"
        ),
    }
    .into()
}

/// STRATA ARE TEXTUAL.
pub(crate) fn recursion_aggregate() -> Refusal {
    Recursion::Aggregate {
        message: "aggregation over the frontier inside a recursive clause: aggregate after the \
                  fixpoint, or carry a running value in the frontier row"
            .to_string(),
    }
    .into()
}

/// LINEARITY.
pub(crate) fn recursion_nonlinear() -> Refusal {
    Recursion::Nonlinear {
        message: "a recursive clause references its target more than once".to_string(),
    }
    .into()
}

/// A self-reference whose actuals are not the instance's own
/// (recursion-contract-law.md, MONOMORPHIC PARAMETERS).
pub(crate) fn parameter_widening(definition: &str) -> Refusal {
    Recursion::ParameterWidening {
        message: format!(
            "'{definition}' refers to itself with a different actual: the actuals select the \
             fixpoint instance and stay invariant for its lifetime; carry changing state in the \
             recursive relation's published columns"
        ),
    }
    .into()
}

/// A reference that returns to a definition being built through another
/// definition (recursion-contract-law.md, CYCLES THROUGH OTHER DEFINITIONS
/// REFUSE).
/// `local` names the closing definition when it is declared in a query's
/// block.
pub(crate) fn recursion_cycle(chain: &str, local: Option<&str>) -> Refusal {
    Recursion::Mutual {
        message: match local {
            Some(closing) => format!(
                "circular expansion of the query-scoped definition '{closing}': mutual recursion is not \
                 supported; the definition-instance cycle is {chain}. Break the cycle, or combine the \
                 state into one recursive relation"
            ),
            None => format!(
                "circular consulted-definition expansion: mutual recursion is not supported; the \
                 definition-instance cycle is {chain}"
            ),
        },
    }
    .into()
}

/// A smart union whose arms publish different name sets.
pub(crate) fn set_name_mismatch() -> Refusal {
    SetOperation::ColumnNameMismatch {
        message: "smart union (|;|) requires every operand to publish the same names, and one operand \
                  does not publish every name the result has; a column whose name was minted (two \
                  columns wanted it, or nobody named it) answers to no name, so baptize it with `as`"
            .to_string(),
    }
    .into()
}

/// A minus step whose arms do not publish the same names (THE MINUS LAW;
/// A MINT ALIGNS WITH NOTHING).
pub(crate) fn minus_names() -> Refusal {
    delightql_types::diagnostic::ResolutionSetop::MinusHeading {
        message: "minus (-) requires both arms to publish the same names; where they differ, rename an arm \
                  first, and a column whose name was minted (two columns wanted it, or nobody named it) answers \
                  to no name, so baptize it with `as`"
            .to_string(),
    }
    .into()
}

/// A positional set operation whose arms display different widths.
pub(crate) fn set_width_mismatch(left: usize, right: usize) -> Refusal {
    SetOperation::ColumnCountMismatch {
        message: format!(
            "Set operation requires both sides to have the same number of columns, but left has {left} \
             and right has {right}"
        ),
    }
    .into()
}

/// A release across arms whose interior headings are not compatible.
pub(crate) fn mixed_release(name: &str) -> Refusal {
    Effect::LedgerMixedRelease {
        message: format!(
            "releasing '{name}' across arms whose interior headings are not structurally \
             compatible"
        ),
    }
    .into()
}

/// A name that reaches a latent dimension.
pub(crate) fn latent_name(name: &str) -> Refusal {
    Semantic::InchoateLatentName {
        message: format!(
            "the dimension is latent: '{name}' names nothing until the occurrence is accessed; \
             reach it by position or access the relation"
        ),
    }
    .into()
}

/// Two completions of one configured value joined in one run: whether
/// they pair by value or by construction row is unruled (owner question
/// C4), so the statement is outside the fragment, not a product limitation.
pub(crate) fn completion_pairing() -> Refusal {
    outside(
        "two relations carrying one configured value's construction rows joined in one run \
         (whether they pair by value or by construction row is unruled)",
    )
}

/// A configured value spent over rows that no longer say which
/// construction row each is (a reduction dropped its lineage): today's
/// refusal, kept behind the O-OCC-8 switch.
pub(crate) fn residual_completion() -> Refusal {
    Ho::ResidualCompletion {
        message: "a residual rule value is completed by rows that lost their construction rows"
            .to_string(),
    }
    .into()
}

/// A mutation whose source marks no occurrence of the target.
pub(crate) fn marker_missing(verb: &str) -> Refusal {
    DmlMarker::Missing {
        message: format!("{verb} requires !! on the source relation that will be mutated"),
    }
    .into()
}

/// A mutation whose source marks more than one occurrence.
pub(crate) fn marker_multiple() -> Refusal {
    DmlMarker::Multiple {
        message: "the source marks more than one occurrence with !!; one statement mutates one \
                  relation"
            .to_string(),
    }
    .into()
}

/// An act in an enclosed position (THE EFFECT FENCE): effects stand only
/// as direct operands of a chain's join.
pub(crate) fn effect_fence(position: &str) -> Refusal {
    Effect::CompilePurity {
        message: format!(
            "a directive stands in {position}, an enclosed position: effects are legal only as direct \
             operands of a chain's join; land the effect's result in a temp object and read that"
        ),
    }
    .into()
}

/// A release of a receipt whose directive declares no payload (THE UNWRAP
/// PIPE: a category error; the receipt continues through `|>`).
pub(crate) fn no_payload(column: &str) -> Refusal {
    Directive::ReceiptNoPayload {
        message: format!(
            "the receipt declares no '{column}' payload, so there is nothing to release: continue the \
             receipt itself with `|>` instead of `!>`"
        ),
    }
    .into()
}

/// A creation whose source reads another connection than its target's
/// (materialization-law §2: creation never transports rows between
/// connections).
pub(crate) fn creation_connection(verb: &str, target: &str, target_connection: i64, source_connection: i64) -> Refusal {
    EffectDdl::TargetNamespace {
        message: format!(
            "{verb}({target}) refuses: the target lives on connection {target_connection}, but the source reads \
             from connection {source_connection}; creation does not move rows between connections \
             (materialization-law §2)"
        ),
    }
    .into()
}

/// A creation whose source reads `sys::` rows: where those rows are
/// physically visible to a creation is docketed (materialization-law §2,
/// requirement 4), so the creation refuses without claiming a settled rule.
pub(crate) fn creation_sys_source(verb: &str, target: &str, namespace: &str) -> Refusal {
    EffectDdl::TargetNamespace {
        message: format!(
            "{verb}({target}) refuses: its source reads {namespace} rows, and where `sys::` rows are physically \
             visible to a creation is an open docket question (materialization-law §2, requirement 4), so no \
             transport of them into the target is authorized"
        ),
    }
    .into()
}

/// A durable creation whose name exists (NAME CLASH; `table_replace!` is
/// reserved for replacement).
pub(crate) fn durable_clash(verb: &str, target: &str) -> Refusal {
    EffectDdl::DurableClash {
        message: format!(
            "{verb}({target}) refuses: '{target}' already exists, and replacement of a durable object must \
             be worn in the name (table_replace! is reserved for that intent)"
        ),
    }
    .into()
}

/// A session creation whose physical temp name another durable owner's
/// session object holds (NAME CLASH: one connection, one temp name pool).
pub(crate) fn temp_name_held(verb: &str, target: &str, held: &str, owner: Option<&str>) -> Refusal {
    EffectDdl::TempNameHeld {
        message: format!(
            "{verb}({target}) refuses: its temp name is held by {held} for {}; a creation never replaces another \
             namespace's session object",
            owner.unwrap_or("a namespace no longer in the catalog")
        ),
    }
    .into()
}

/// A creation target that is not a whole-relation designator.
pub(crate) fn creation_designator(verb: &str) -> Refusal {
    EffectDdl::TargetDesignator {
        message: format!(
            "{verb}'s target is a DESIGNATOR — `name()` or `name(*)` — naming what to create; filters, \
             projections and derived relations do not belong in a target"
        ),
    }
    .into()
}

/// An effect rule whose family has a ground member in a clause's parameter
/// row (AN EFFECT RULE HAS NO GROUND MEMBER).
pub(crate) fn effect_ground_member(rule: &str) -> Refusal {
    Effect::TransformUnsupported {
        message: format!(
            "{rule} has a ground member in its parameter row: effect clauses are not dispatched by ground \
             value, so the rule refuses before any clause runs"
        ),
    }
    .into()
}

/// An effect rule reached again while it is invoked (NO RECURSION).
pub(crate) fn effect_recursion(rule: &str) -> Refusal {
    EffectRule::Recursion {
        message: format!(
            "effect rule '{rule}' must not recurse, directly or transitively: it invokes itself, and every effect \
             rule expands to a finite plan"
        ),
    }
    .into()
}

/// An effect rule clause that does not end in a receipt-producing
/// disposition (THE ENDING LAW).
pub(crate) fn effect_ending(rule: &str) -> Refusal {
    EffectRule::Ending {
        message: format!(
            "effect rule '{rule}': its body must end in a directive — a clause ends in a receipt-producing \
             disposition: end it with `|>` into a directive, or package an ordinary relation with \
             `|> returning!(*)`"
        ),
    }
    .into()
}

/// An effect body that demands a session directive (THE SESSION'S SHAPE IS
/// TEXT, NOT DATA).
pub(crate) fn effect_session_directive(rule: &str, directive: &str) -> Refusal {
    EffectBody::SessionDirective {
        message: format!(
            "{rule}'s body demands the session directive '{directive}': a session directive alters what a \
             statement is compiled against, so it is legal only in the liminal space and at the REPL/CLI top \
             level, never in an effect body"
        ),
    }
    .into()
}

/// An effect body that demands `run!`, which consults its file during the
/// run (THE SESSION'S SHAPE IS TEXT, NOT DATA).
pub(crate) fn effect_run_in_body(rule: &str) -> Refusal {
    EffectBody::Run {
        message: format!(
            "{rule}'s body demands run!, which consults its file while the run executes, and a run never changes \
             the world it was compiled against: consult the file before the run and demand run_namespace! from \
             the body"
        ),
    }
    .into()
}

/// A `main!` with more than one clause.
pub(crate) fn effect_main_multi_clause(clauses: usize) -> Refusal {
    EffectMain::MultiClause {
        message: format!(
            "effect rule 'main!' may only be single-claused, whichever neck declares it: found {clauses} clauses"
        ),
    }
    .into()
}

/// A rule-valued parameter whose actual names no rule.
pub(crate) fn rule_value_missing(callee: &str, name: &str) -> Refusal {
    Ho::RuleValueMissing {
        message: format!("{callee}'s rule-valued parameter receives '{name}', which names no rule"),
    }
    .into()
}

/// A user directive no effect rule answers.
pub(crate) fn directive_unknown(spelled: &str) -> Refusal {
    Resolution::CallableUnknown {
        message: format!("directive '{spelled}' is not a built-in directive, and no effect rule answers it"),
    }
    .into()
}

/// A mutation target the statement's query-local blocks claim.
pub(crate) fn dml_target_local(verb: &str, name: &str) -> Refusal {
    DmlRoles::Target {
        message: format!(
            "{verb}'s target '{name}' names a relation this statement computes, not a stored table: a \
             mutation writes one stored table"
        ),
    }
    .into()
}

/// A mutation target that is no stored table of the session.
pub(crate) fn dml_target_not_table(verb: &str, name: &str) -> Refusal {
    DmlRoles::Target {
        message: format!("DML target '{name}' is not a physical table: {verb} writes one stored table"),
    }
    .into()
}

/// A marked read of a relation that is no stored table: a view, the
/// catalog's or a DQL definition, is never a mutation's target.
pub(crate) fn marked_not_table(name: &str) -> Refusal {
    DmlRoles::Target {
        message: format!(
            "'{name}' is not a physical table, so no mutation reaches its rows: insert!, update! and delete! \
             write one stored table"
        ),
    }
    .into()
}

/// An actual whose kind is not its formal's: a relation where the formal
/// takes a value, or a value where it takes a relation.
pub(crate) fn argument_kind(entity: &str, formal: &str) -> Refusal {
    Ho::RelationalArgument {
        message: format!("the actual of '{entity}'s parameter '{formal}' is not of the kind the parameter declares"),
    }
    .into()
}

/// A rule value whose configured actuals leave no relation input open.
pub(crate) fn residual_none(rule: &str, supplied: usize, declared: usize) -> Refusal {
    Ho::ResidualFrontier {
        message: format!(
            "'{rule}' supplies {supplied} prefix actual(s) for {declared} declared position(s), leaving no residual"
        ),
    }
    .into()
}

/// A rule value whose residual does not leave the inputs its position
/// requires.
pub(crate) fn residual_contract(rule: &str, position: &str) -> Refusal {
    Ho::ResidualContract {
        message: format!("the residual of '{rule}' does not leave the one relation input {position} requires"),
    }
    .into()
}

/// A designated family whose residual is not the contract of the position
/// it stands in: other remaining positions, or another published heading
/// (FN.48: wrong residual contract).
pub(crate) fn residual_contract_mismatch(rule: &str) -> Refusal {
    Ho::ResidualContract {
        message: format!(
            "the residual of '{rule}' has a different remaining mode or published heading than the rule-valued \
             parameter it stands at requires"
        ),
    }
    .into()
}

/// A family whose clauses do not state one parameter row: its length, what
/// a position receives, or the context it captures (heads-law CLAUSE
/// AGREEMENT: `param_arity` covers roles, positions and capture).
pub(crate) fn parameter_rows_differ(subject: &str, disagreement: &str) -> Refusal {
    delightql_types::diagnostic::DdlHead::ParamArity {
        message: format!(
            "'{subject}' is one family with one parameter row, but {disagreement}. Each clause names its own scalar \
             formals, but every clause declares the same number of parameters, receives the same kind of argument at \
             each position, and captures the same context"
        ),
    }
    .into()
}

/// A rule-valued argument position whose clauses do not state one contract
/// there (FN.48).
pub(crate) fn rule_contracts_differ(subject: &str, position: usize, statements: &str) -> Refusal {
    delightql_types::diagnostic::DdlHead::RuleContract {
        message: format!(
            "'{subject}': argument position {position} is rule-valued, and its clauses do not state one contract \
             there ({statements}). Every clause receives a rule value at that position, with the same remaining \
             positions in order and the same published heading"
        ),
    }
    .into()
}

/// A family some of whose clauses wear the fixpoint badge and some do not
/// (recursion-contract-law THE BADGE CHOOSES THE UNION).
pub(crate) fn badges_differ(subject: &str, statements: &str) -> Refusal {
    delightql_types::diagnostic::Recursion::MixedBadge {
        message: format!(
            "'{subject}': its clauses disagree about the fixpoint badge ({statements}). The badge chooses one \
             accumulation for the whole definition: badge every clause, or none"
        ),
    }
    .into()
}

/// `_` or `@` in a designator's configured prefix (FN.48: neither authors a
/// future hole; a residual binds a complete left prefix).
pub(crate) fn residual_prefix(rule: &str) -> Refusal {
    Ho::ResidualPrefix {
        message: format!(
            "'{rule}' is designated with `_` or `@` in its configured prefix: a residual binds a complete left \
             prefix, and neither defers a position"
        ),
    }
    .into()
}

/// A piped relation with no formal left to land in (STRICT LANDING).
pub(crate) fn landing_nowhere(callee: &str) -> Refusal {
    Effect::LandingNowhere {
        message: format!(
            "{callee}: every parameter is already written, so the piped relation has nowhere to land; \
             piped data must land, visibly, exactly once"
        ),
    }
    .into()
}

/// A marked occurrence under a terminal that reads no row it writes.
pub(crate) fn marker_forbidden(verb: &str) -> Refusal {
    DmlMarker::Forbidden {
        message: format!(
            "{verb} reads no row of its target, so its source carries no `!!` occurrence: the \
             target parameter alone declares the write"
        ),
    }
    .into()
}

/// An insertion source column that names no column of the target.
pub(crate) fn insert_unnamed_column(column: &str) -> Refusal {
    Dml::InsertUnnamedColumn {
        message: format!(
            "insert! writes each source column into the target column of its name, and '{column}' \
             names no column of the target (or one another source column already names)"
        ),
    }
    .into()
}

/// A marked occurrence that reads another relation than the target.
pub(crate) fn marker_mismatch(verb: &str, marked: &str, target: &str) -> Refusal {
    DmlMarker::Mismatch {
        message: format!("!! source table '{marked}' does not match {verb} target '{target}'"),
    }
    .into()
}

/// The statement's target exposes no row locator.
pub(crate) fn row_identity(name: &str) -> Refusal {
    Dml::TargetRowIdentity {
        message: format!(
            "`{name}!!` selects rows a mutation reaches by the target's own row locator, and \
             the statement's target exposes none"
        ),
    }
    .into()
}

/// A form between the marked occurrence and the terminal dropped its rows'
/// locator.
pub(crate) fn source_occurrence(verb: &str) -> Refusal {
    DmlSource::Occurrence {
        message: format!(
            "{verb} reaches the rows the `!!` occurrence selected by that occurrence's row \
             identity, and the source no longer carries it: a form whose rows are not the \
             occurrence's own stands between the marker and the terminal"
        ),
    }
    .into()
}

/// A reduction among the stages the terminal consumes.
pub(crate) fn source_aggregate(verb: &str) -> Refusal {
    DmlSource::Aggregate {
        message: format!(
            "cannot aggregate before {verb}: aggregation changes the row identity, so its rows \
             map back to no source row"
        ),
    }
    .into()
}

/// A source whose rows the marked occurrence reaches through a bound: the
/// rows changed would depend on an unstated order.
pub(crate) fn bounded_mutation(verb: &str) -> Refusal {
    DmlShape::BoundedMutation {
        message: format!(
            "{verb} cannot consume a relation bounded by position (`# < N`): no exact bounded \
             mutation is admitted — filter the source instead"
        ),
    }
    .into()
}

/// An update whose incoming heading is not the target's heading by name.
pub(crate) fn update_heading(detail: &str) -> Refusal {
    DmlShape::UpdateHeading {
        message: format!(
            "update! replaces each row it reaches by its incoming row, so the incoming heading \
             must be the target's heading, by name, in any order: {detail}"
        ),
    }
    .into()
}

/// A deletion's source that does not publish its table's heading by name:
/// the rows it deletes are those the marked read located, and its source
/// is judged by the shape rule an update's is.
pub(crate) fn delete_heading(detail: &str) -> Refusal {
    DmlShape::DeleteWithCover {
        message: format!(
            "delete! removes the rows its marked read located, and its source must publish the target's \
             heading, by name, in any order, as an update's does: {detail}"
        ),
    }
    .into()
}

/// An insertion naming a column its table generates: generated columns are
/// read-only.
pub(crate) fn generated_insert(column: &str) -> Refusal {
    Dml::InsertUnnamedColumn {
        message: format!(
            "insert! writes each source column into the target column of its name, and '{column}' names a \
             column the table generates, which is read-only: leave it out and the table computes it"
        ),
    }
    .into()
}

/// A mutation call that does not name exactly one target relation.
pub(crate) fn dml_target_count(named: usize) -> Refusal {
    DmlRoles::Target {
        message: format!("a mutation writes one relation; this call names {named}"),
    }
    .into()
}

/// A mutation call that does not read exactly one source relation.
pub(crate) fn dml_source_count(named: usize) -> Refusal {
    DmlRoles::Source {
        message: format!("a mutation reads one relation; this call names {named}"),
    }
    .into()
}

/// A rule value in a mutation's argument group.
pub(crate) fn dml_rule_value() -> Refusal {
    DmlRoles::RuleValue {
        message: "a mutation role requires a relation, not a residual rule value".to_string(),
    }
    .into()
}

/// A target that is not a whole-table designator.
pub(crate) fn target_designator(verb: &str) -> Refusal {
    Effect::DmlTargetDesignator {
        message: format!(
            "{verb}'s target is a whole-table DESIGNATOR — `name(*)`, optionally \
             namespace-qualified — naming where to write; filters, projections and derived \
             relations do not belong in a target"
        ),
    }
    .into()
}

/// A target in an engine-owned namespace.
pub(crate) fn engine_owned(target: &str, namespace: &str) -> Refusal {
    Effect::TargetEngineOwned {
        message: format!(
            "DML target '{target}' resolves into the engine-owned namespace '{namespace}': \
             programs cannot mutate system relations — query it, never write it"
        ),
    }
    .into()
}

/// Receipt access that is neither whole nor an exact positional list.
pub(crate) fn receipt_access(verb: &str, width: usize) -> Refusal {
    Directive::InvocationAccess {
        message: format!(
            "the visible group on {verb} is RECEIPT access, not its arguments: receipt access is a positional \
             list of plain names with {width} column(s), or (*). Write the argument group first: \
             {verb}(arguments)(*)"
        ),
    }
    .into()
}

/// A condition reading a merged key an outer join publishes, placed
/// anywhere but the join of a member after the merge's: whether it filters
/// the run or is the merge's own match condition is unruled.
pub(crate) fn merged_key_condition() -> Refusal {
    outside(
        "a condition on a merged key an outer join publishes, other than a later member's match \
         condition (whether it filters the run or matches the merge's own join is unruled)",
    )
}

/// A document operation aimed at a column declared numeric, date or
/// boolean (THE DOCUMENT DENYLIST: one classifier, one URI).
pub(crate) fn scalar_column(consumer: &str) -> Refusal {
    let what = if consumer == "a path" {
        "a plain scalar has no insides to reach into"
    } else {
        "a plain scalar has no rows to iterate"
    };
    Semantic::CompoundScalarColumn {
        message: format!(
            "{consumer} aims at a column declared numeric, date or boolean: {what}. Aim it at a structure built \
             with {{…}}/[…], a tree group, or a document column (TEXT)"
        ),
    }
    .into()
}

/// A pattern that binds one name twice: the bindings share one heading,
/// so a second is not another extraction.
pub(crate) fn pattern_duplicate(name: &impl std::fmt::Display) -> Refusal {
    Constraint::General {
        message: format!("the destructure pattern binds '{name}' more than once: a pattern binds each column once"),
    }
    .into()
}

/// An extracted value read back across a definition, CTE, stored-relation
/// or effect-staging boundary and handed to a structured consumer: the
/// boundary carries the cell but no evidence of its kind.
pub(crate) fn dynamic_continuity(consumer: &str) -> Refusal {
    Limitation::DynamicStructuredContinuity {
        message: format!(
            "{consumer} reads a value a path extracted on the other side of a read boundary, which carries no \
             evidence of whether the value is a record, a tuple, a scalar or JSON-looking text: carry the source \
             document across the boundary and extract at the last structured consumer"
        ),
    }
    .into()
}

/// An act staging a relation that holds a value whose kind no evidence
/// proves: effect staging is conservative at the boundary itself.
pub(crate) fn dynamic_staging() -> Refusal {
    Limitation::DynamicStructuredContinuity {
        message: "an effect staging would hold a value a path extracted, with no evidence of whether it is a record, \
                  a tuple, a scalar or JSON-looking text, and a staging admits no such value: carry the source \
                  document across the act and extract at the last structured consumer"
            .to_string(),
    }
    .into()
}

/// A record whose one key two different cells supply: engines disagree on
/// duplicate keys, so it never reaches lowering.
pub(crate) fn key_collision(key: &impl std::fmt::Display) -> Refusal {
    Constraint::General {
        message: format!(
            "the record names key '{key}' for two different values: a key is an address within one value, so \
             each may name only one. Rename one member with \"key\": value"
        ),
    }
    .into()
}

/// A sequence position over a value every row of which is one record the
/// language made: narrowing iterates a sequence.
pub(crate) fn object_literal(column: &str) -> Refusal {
    Narrowing::ObjectLiteral {
        message: format!(
            "narrowing iterates an array, and every row of '{column}' is one record: path into the record \
             instead ({column}:{{.field}}), or make a one-element sequence ([{{…}}])"
        ),
    }
    .into()
}

/// A statement whose answer depends on a semantic question the owner has
/// not ruled: refused under the question's own words, never answered by a
/// reading the law does not state.
pub(crate) fn unruled(question: &str) -> Refusal {
    Operational::Uncovered {
        message: format!("DelightQL does not answer an unruled question: {question} is not ruled"),
    }
    .into()
}

/// A table a user receives (the statement's result, a created view or
/// table) of zero columns on a target whose SQL cannot hold one (top-grammar
/// FN.14, ZERO WIDTH IS LAWFUL: a placeholder column exists only inside
/// generated text, never in a table a user receives).
pub(crate) fn zero_width_table(what: &str, target: &str) -> Refusal {
    Constraint::TargetCapability {
        message: format!(
            "{what} has no column, and {target} cannot hold a table of zero columns: a column written to stand in \
             for it would be a value the table holds. To ask whether a row exists, keep a witness column \
             (`|> (1 as found)`); to ask how many, count them (`|> %(~> count:(*) as n)`)"
        ),
    }
    .into()
}

/// Whether a refusal says only that this middle does not cover a form (or
/// holds an unruled question): what a declaration judged over stand-ins
/// leaves to the uses.
pub(crate) fn is_uncovered(refusal: &Refusal) -> bool {
    matches!(refusal, DelightQLError::Operational(Operational::Uncovered { .. }))
}

/// Whether a refusal judges a definition's self-reference (the recursion
/// contract): a fact of the definition's own structure, decided where it
/// is declared.
pub(crate) fn is_self_reference(refusal: &Refusal) -> bool {
    matches!(
        refusal,
        DelightQLError::Semantic(delightql_types::diagnostic::Semantic::Recursion(_))
    )
}

/// A context marker written where the callee captures no row, or past a
/// call's first argument.
pub(crate) fn context_marker_position(callee: &str) -> Refusal {
    Constraint::Context {
        message: format!(
            "'{callee}' is not context-aware here: `..` captures the caller's row for a function that declares \
             a context, and stands first among its arguments"
        ),
    }
    .into()
}

/// A context marker at a call of a target function: `..` selects the
/// calling mode of a definition that declares a context, which no target
/// function does.
pub(crate) fn context_marker_at_target(callee: &str) -> Refusal {
    Constraint::Context {
        message: format!(
            "'{callee}' declares no context: `..` selects the context calling mode of a definition that declares \
             one, and this call instantiates none"
        ),
    }
    .into()
}

/// The composition input `@` where nothing flows in: no callable stands
/// around it to apply it, so it has no value to become (FN.4).
pub(crate) fn composition_input_unapplied() -> Refusal {
    Semantic::ValueOpenUnapplied {
        message: "`@` names what flows in, and it stands outside any callable applying it".to_string(),
    }
    .into()
}

/// A context call written where no row of the text's own stands: a value
/// function's body has none to capture.
pub(crate) fn context_without_row(callee: &str) -> Refusal {
    Constraint::Context {
        message: format!(
            "`{callee}:(..)` captures the row its call stands in, and a function body has no row of its own: \
             compute the value in the calling function, or pass it explicitly (a declared capture's columns \
             may be written as its first arguments)"
        ),
    }
    .into()
}

/// A callable written where a value definition takes a value.
pub(crate) fn code_argument(callee: &str) -> Refusal {
    delightql_types::diagnostic::Cfe::CodeArgument {
        message: format!("'{callee}' takes a value where a callable was written"),
    }
    .into()
}

/// A value written where a value definition's formal takes a callable.
pub(crate) fn callable_expected(callee: &str, position: usize) -> Refusal {
    delightql_types::diagnostic::Cfe::CodeArgument {
        message: format!(
            "'{callee}' takes a callable at argument {position}: write a function (`upper:()`), a lambda (`:(@ * 2)`) \
             or a template (`:\"<{{@}}>\"`), not a value"
        ),
    }
    .into()
}

/// A callable formal applied to other than its one argument.
pub(crate) fn lambda_arity(callee: &str, supplied: usize) -> Refusal {
    delightql_types::diagnostic::Cfe::LambdaArity {
        message: format!("the callable formal '{callee}' applies to one value, got {supplied}"),
    }
    .into()
}

/// A fact function's call selecting an output its mode does not declare.
pub(crate) fn mode_unknown_output(function: &str, output: &str, outputs: &[String]) -> Refusal {
    delightql_types::diagnostic::Mode::UnknownOutput {
        message: format!("'{function}' declares no output '{output}' — its outputs are {}", dotted(outputs)),
    }
    .into()
}

/// A fact function's outputs as a call selects them: `.a, .b`.
fn dotted(outputs: &[String]) -> String {
    outputs.iter().map(|o| format!(".{o}")).collect::<Vec<_>>().join(", ")
}

/// A fact function called with other than its declared inputs.
pub(crate) fn fact_arity(function: &str, declared: usize, supplied: usize) -> Refusal {
    delightql_types::diagnostic::Cfe::Arity {
        message: format!("'{function}' declares {declared} inputs, and the call supplies {supplied}"),
    }
    .into()
}

/// A fact function of several outputs called in value position without
/// selecting one: its call is a row, not a value.
pub(crate) fn mode_degree(function: &str, outputs: &[String]) -> Refusal {
    delightql_types::diagnostic::Mode::Degree {
        message: format!(
            "'{function}:(…)' is one ROW of {} outputs, and a value position holds one column — pick one: {}",
            outputs.len(),
            dotted(outputs)
        ),
    }
    .into()
}

/// An output selected from a function that declares no mode.
pub(crate) fn mode_undeclared(function: &str) -> Refusal {
    delightql_types::diagnostic::Mode::Undeclared {
        message: format!("'{function}' declares no functional mode, so its call has no output to select"),
    }
    .into()
}

/// A fact function with a default read as a relation: it has no finite
/// relational face.
pub(crate) fn fact_relational_face(function: &str) -> Refusal {
    Resolution::FactFunctionRelationalFace {
        message: format!(
            "'{function}' declares a default (`_ ->`), so it answers every input and has no finite relational \
             face: call it, as `{function}:(…)`"
        ),
    }
    .into()
}

/// A form the middle does not yet support. Not a semantic refusal: the
/// statement uses a form not yet built.
pub(crate) fn outside(what: &str) -> Refusal {
    Operational::Uncovered {
        message: format!("DelightQL does not yet support {what}"),
    }
    .into()
}

/// A consumer that reads a document's bytes (a stored write, a call, a
/// cast, an operator, a comparison, a grouping, distinct or ordering key)
/// over a document holding a key the compiler minted: the
/// key's spelling is drawn per compilation, so no answer may depend on it
/// (MINTED SPELLINGS ARE INVARIANT). A member read by its position is not
/// such a consumer.
pub(crate) fn minted_bytes(consumer: &str) -> Refusal {
    outside(&format!("{consumer} of a document whose member no key names (its key is minted)"))
}

/// A contract of the middle's realization broken: a defect it
/// caught, never a boundary of what it covers.
pub(crate) fn contract(what: &str) -> Refusal {
    Internal::invariant("middle-end realization", what)
}

/// State the elaborator keeps found inconsistent with itself: a defect it
/// caught, never a boundary of what it covers.
pub(crate) fn elaboration_contract(what: &str) -> Refusal {
    Internal::invariant("middle-end elaboration", what)
}

/// A window evaluated directly by a condition assigned to a join
/// (predicate-placement-law: population evaluation and condition placement).
pub(crate) fn window_on_join(what: &str) -> Refusal {
    Window::OnJoin {
        message: format!(
            "{what} evaluates a window, and a join evaluates its condition per pair of rows, where \
             a window has no population: compute the window in an earlier stage or a separate \
             relation, then join on its column"
        ),
    }
    .into()
}

/// A condition relating two members written before a population-sensitive
/// step, itself written after it: whether the step's population is their
/// join with it (the join's `ON`) or without it (a later filter) is unruled.
pub(crate) fn condition_after_population_step() -> Refusal {
    outside(
        "a condition relating members written before a window, an aggregate or a bound, itself \
         written after it (whether the step's population includes the condition is unruled)",
    )
}

/// A `min_multiplicity` gate no correlated union in the statement spends
/// (set-operations-law: an acknowledged danger the query never exercises).
pub(crate) fn min_multiplicity_unspent() -> Refusal {
    delightql_types::diagnostic::Setop::MinMultiplicityUnspent {
        message: "the min_multiplicity gate changes only a correlated union, and no correlated union in this \
                  statement spends it: the gate does nothing here. Remove it, or write it beside the correlated \
                  union whose pairing it changes"
            .to_string(),
    }
    .into()
}

/// A `min_multiplicity` gate over a correlation that compares only some of
/// the columns the arms share (set-operations-law: the gate pairs whole
/// rows).
pub(crate) fn min_multiplicity_partial() -> Refusal {
    delightql_types::diagnostic::Setop::MinMultiplicityPartial {
        message: "the min_multiplicity gate pairs whole rows, and this correlation compares only some of the \
                  columns the arms share, so which of several copies under one key pairs up would be decided by \
                  row order: correlate every shared column (x.* = y.*)"
            .to_string(),
    }
    .into()
}

/// A correlation under the `min_multiplicity` gate that compares by an
/// operator other than equality: the gate pairs equal rows.
pub(crate) fn min_multiplicity_operator() -> Refusal {
    delightql_types::diagnostic::Setop::MinMultiplicityCorrelationOperator {
        message: "the min_multiplicity gate pairs copies of equal rows, and this correlation compares by another \
                  operator: correlate every shared column by equality (x.* = y.*)"
            .to_string(),
    }
    .into()
}

/// A bare name in a condition after a set step that names an arm, carried
/// by no arm entered before it or by more than one (THE ONE-RELATION LAW:
/// name one arm or none).
pub(crate) fn arm_condition_bare(name: &impl std::fmt::Display, carriers: usize) -> Refusal {
    let carried = match carriers {
        0 => "carried by no operand".to_string(),
        _ => "carried by more than one operand".to_string(),
    };
    delightql_types::diagnostic::ResolutionSetop::CorrelationOwner {
        message: format!(
            "'{name}' in a condition that names an arm of the set operation is {carried}; a condition after a \
             set step names one arm or none, so qualify it with the arm it addresses"
        ),
    }
    .into()
}

/// A whole-heading correlation over arms that share no name: there is
/// nothing to compare.
pub(crate) fn whole_correlation_empty(left: &impl std::fmt::Display, right: &impl std::fmt::Display) -> Refusal {
    Constraint::SelectorEmpty {
        message: format!(
            "{left}.* = {right}.* compares every name both arms publish, and they share none: there is nothing to \
             compare; name the columns the correlation compares"
        ),
    }
    .into()
}

/// An arm's name read after its set step anywhere but a condition: the
/// step's result is one relation, and no row of it says which arm it came
/// from.
pub(crate) fn arm_name_after(spelled: &str) -> Refusal {
    column(
        spelled,
        "an arm's name addresses its rows only in a condition written after its set step; the step's result is \
         one relation",
    )
}

/// TWO LIVE SCOPES NEVER SHARE A NAME (addressing-matrix-law).
pub(crate) fn scope_duplicate(name: &impl std::fmt::Display) -> Refusal {
    Semantic::ScopeDuplicate {
        message: format!(
            "two live scopes cannot share the name '{name}': a scope name borne twice addresses \
             nothing; alias one side with `as`"
        ),
    }
    .into()
}

/// An explicit item publishing a name a glob item of the same stage also
/// publishes (DUPLICATE AUTHORED NAMES REFUSE; A GLOB DOES NOT SKIP).
pub(crate) fn glob_collision(name: &impl std::fmt::Display) -> Refusal {
    Constraint::General {
        message: format!(
            "Duplicate column '{name}': an explicit item and a glob item both publish it; rename \
             the explicit item with 'as' or remove it"
        ),
    }
    .into()
}

/// A reducing call standing in a row-wise position
/// (domain-expressions-grammar: THE CALL POSITION ASCRIBES GRADE).
pub(crate) fn implicit_aggregation(callee: &str) -> Refusal {
    Constraint::ImplicitAggregation {
        message: format!(
            "`{callee}:` reduces the rows it stands over, and it stands in a row-wise position: \
             reduce it in a grouping (`%( ~> {callee}:(…))`) or window it"
        ),
    }
    .into()
}

/// A value with one answer per row standing alone in a reduction item,
/// which stands for its group's many rows (no implicit aggregation, ever).
pub(crate) fn per_row_in_reduction(value: &str) -> Refusal {
    Constraint::ImplicitAggregation {
        message: format!(
            "{value} has one answer per row and stands alone in a reduction, which stands for the \
             group's many rows: reduce it, or group by it; there is no implicit aggregation"
        ),
    }
    .into()
}

/// A known window function standing where no window is written.
pub(crate) fn needs_window(callee: &str) -> Refusal {
    Window::NeedsWindow {
        message: format!(
            "'{callee}' is a window function and computes over a window; standing bare it has \
             nothing to compute over"
        ),
    }
    .into()
}

/// A glob over a scope holding a latent dimension: a glob does not skip,
/// and no name reaches a latent dimension (the refusal is today's; the
/// question of a glob activating it is open).
pub(crate) fn latent_under_glob() -> Refusal {
    Semantic::InchoateLatentName {
        message: "the dimension is latent: a glob covers every cell and none is skipped, and a \
                  latent dimension names nothing until the occurrence is accessed; access the \
                  relation or reach the dimension by position"
            .to_string(),
    }
    .into()
}

/// A sigma name no truth rule or served predicate answers.
pub(crate) fn callable_unknown_sigma(name: &str) -> Refusal {
    Resolution::CallableUnknown {
        message: format!(
            "no sigma callable '{name}' is reachable. A sigma name must resolve to an authored \
             sigma rule or a served sigma predicate"
        ),
    }
    .into()
}

/// A sigma citation with the wrong number of arguments.
pub(crate) fn sigma_arity(name: &str, width: usize, written: usize) -> Refusal {
    Semantic::Arity {
        message: format!("{name} expects {width} arguments, got {written}"),
    }
    .into()
}

/// A query-mode family's anchor reading its own subject: the name names
/// nothing yet available (THE ANCHOR COMES FIRST).
pub(crate) fn anchor_unresolved(name: &str) -> Refusal {
    Resolution::CallableUnknown {
        message: format!(
            "query-local name '{name}' is visible here, but its common table expression is not \
             available while its own anchor is read: put the base clause first"
        ),
    }
    .into()
}

/// A definition that can see its own name reaching itself before a base
/// clause established its fixpoint (THE ANCHOR COMES FIRST).
pub(crate) fn anchor_first(name: &str, local: bool) -> Refusal {
    Recursion::AnchorFirst {
        message: match local {
            true => format!(
                "the common higher-order expression '{name}' reaches itself before any base clause \
                 established its fixpoint. Its base (non-recursive) clause must come first: a \
                 self-reference is recursive only once a prior clause has established the name"
            ),
            false => format!(
                "circular consulted-definition expansion: '{name}' reaches itself before any base \
                 clause established its fixpoint. The base (non-recursive) clause must come FIRST in \
                 the consulted file: a self-reference is recursive only once a prior clause has \
                 established the name"
            ),
        },
    }
    .into()
}

/// A self-reference that rebinds or constrains the frontier (THE FRONTIER
/// BINDS BY ITS OWN HEADING).
pub(crate) fn argumentative_binding() -> Refusal {
    Recursion::ArgumentativeBinding {
        message: "a self-reference reads the frontier whole, under the heading the definition \
                  publishes: write `(*)` and rename or filter in a stage"
            .to_string(),
    }
    .into()
}

/// A self-reference inside a subquery of its own recursive clause (NO
/// SUBQUERY AGAINST THE TARGET).
pub(crate) fn self_subquery() -> Refusal {
    Recursion::SelfSubquery {
        message: "the recursive relation is referenced inside a subquery of its own recursive \
                  clause: a recursive clause sees only the previous iteration's rows, as a direct \
                  source"
            .to_string(),
    }
    .into()
}

/// A badge on a definition that never refers to itself.
pub(crate) fn false_fixpoint(name: &str) -> Refusal {
    Recursion::FalseFixpoint {
        message: format!(
            "'{name}' wears the deduplicating fixpoint badge `%` and references nothing of itself; \
             spell a distinct view in the body instead"
        ),
    }
    .into()
}

/// A value function that reaches itself (RELATION-FORM ONLY).
pub(crate) fn recursion_function_form(name: &str) -> Refusal {
    Recursion::FunctionForm {
        message: format!(
            "'{name}' reaches itself as a value function: define the relation, ground the argument"
        ),
    }
    .into()
}

/// A truth rule that cites itself (RELATION-FORM ONLY).
pub(crate) fn recursion_truth_form(name: &str) -> Refusal {
    Recursion::TruthForm {
        message: format!(
            "'{name}' cites itself as a truth rule: write the recursion as a relational rule and \
             test the relation"
        ),
    }
    .into()
}

/// Clauses of one family that publish different headings (heads-law:
/// CLAUSE AGREEMENT).
pub(crate) fn clause_disagreement(name: &str, first: &[String], clause: usize, other: &[String]) -> Refusal {
    Semantic::HeadsClauseDisagreement {
        message: format!(
            "the clauses of '{name}' publish different headings: clause 1 publishes ({}) and \
             clause {clause} publishes ({}); every clause must publish one heading",
            first.join(", "),
            other.join(", ")
        ),
    }
    .into()
}

/// A family of closed heads (listed heads, facts) whose clauses publish
/// different widths (heads-law CLAUSE AGREEMENT: different arities under one
/// name are a mismatch, not an overload).
pub(crate) fn head_arity(name: &str, width: usize, clause: usize, other: usize) -> Refusal {
    delightql_types::diagnostic::DdlHead::Arity {
        message: format!(
            "the clauses of '{name}' must have the same arity: clause 1 publishes {width} position(s) and \
             clause {clause} publishes {other}; one name is one entity with one arity"
        ),
    }
    .into()
}

/// A family of closed heads whose clauses offer two names for one position
/// (heads-law CLAUSE AGREEMENT: conflicting offers refuse).
pub(crate) fn head_name_conflict(name: &str, position: usize, first: &str, other: &str, clause: usize) -> Refusal {
    delightql_types::diagnostic::DdlHead::NameConflict {
        message: format!(
            "position {position} of '{name}' carries conflicting name offers '{first}' and '{other}' (clause \
             {clause}); a position's public name is the one its offering clauses agree on, so conform the clause \
             that differs with `as`"
        ),
    }
    .into()
}

/// A closed head two of whose positions name one identifier (a head is
/// ordinary projection, and a heading names each column once).
pub(crate) fn head_name_collision(name: &str, earlier: usize, position: usize, spelled: &str) -> Refusal {
    delightql_types::diagnostic::DdlHead::NameCollision {
        message: format!(
            "positions {earlier} and {position} of '{name}' both name '{spelled}': a heading names each column \
             once, so give one of them another name"
        ),
    }
    .into()
}

/// A fact header slot that names nothing (`_`, a constraint term): the
/// header is the fact's head, and every head position is named or
/// supplied.
pub(crate) fn head_names_nothing(name: &str, position: usize) -> Refusal {
    delightql_types::diagnostic::DdlHead::FactHeader {
        message: format!(
            "position {position} of the header of '{name}' names nothing: a fact's header is its head, so each \
             header item is the name of one column"
        ),
    }
    .into()
}

/// A family mixing an open (glob) head with closed heads (heads-law OPEN
/// AND CLOSED: an open head takes its body's heading, a closed head
/// declares it).
pub(crate) fn head_mixed_forms(name: &str) -> Refusal {
    delightql_types::diagnostic::DdlHead::MixedForms {
        message: format!(
            "the clauses of '{name}' mix an open head (`*`) with closed heads: one entity's heading is either \
             its body's or its head's"
        ),
    }
    .into()
}

/// A position every clause of a family supplies with a ground term and none
/// names (heads-law THE GROUND-POSITION RULE).
pub(crate) fn unnamed_ground_position(name: &str, position: usize) -> Refusal {
    delightql_types::diagnostic::DdlHead::UnnamedGroundPosition {
        message: format!(
            "position {position} of '{name}' is supplied only by ground terms and no clause names it; name it \
             in a head with `as` (the literal still supplies, the label names the position)"
        ),
    }
    .into()
}

/// A routed session: the statement's target is late, which the fragment
/// does not cover.
pub(crate) fn routed_session() -> Refusal {
    outside("a statement whose target depends on which connections it reads (a routed session)")
}

/// An aggregate over values another reduction already reduces.
pub(crate) fn nested_reduction(callee: &str) -> Refusal {
    Constraint::NestedReduction {
        message: format!(
            "`{callee}:` reduces values that are already one row of a reduction; a reduction of a \
             reduction needs a relation boundary between the two, written as a stage"
        ),
    }
    .into()
}

/// A full join (of a member that, like every member before it, is marked)
/// that no condition, merge or dependence relates to the members before it.
/// The identity is the product's parse refusal of the same shape.
pub(crate) fn full_outer_unrelated(member: usize) -> Refusal {
    Parse::General {
        message: format!(
            "FULL OUTER JOIN requires an explicit join condition: member {} and every member before \
             it are marked optional (?), which makes its join FULL OUTER, and that join has no \
             condition saying how its two sides align. Add a join condition or give the patterns a \
             shared variable",
            member + 1
        ),
    }
    .into()
}

/// A merge name that selects no single position to the left: the positions
/// that wanted it lost it by collision.
pub(crate) fn correspondence_not_exact(name: &str) -> Refusal {
    Resolution::CorrespondenceNotExact {
        message: format!(
            "the merge name '{name}' does not select exactly one position to the left: several \
             positions there lost it by collision; address one through its qualifier"
        ),
    }
    .into()
}

/// A directive legal only in a consulted file's liminal space, written in a
/// statement (LIMINAL ELIGIBILITY).
pub(crate) fn liminal_only(operation: &str) -> Refusal {
    delightql_types::diagnostic::DirectiveContext::LiminalOnly {
        message: format!("'{operation}' is legal only in the liminal space of a consulted file, not as a query invocation"),
    }
    .into()
}

/// A session directive inside a definition's body: session directives are
/// legal only at the prompt and in a consulted file's liminal space (THE
/// SESSION'S SHAPE IS TEXT, NOT DATA).
pub(crate) fn session_position(operation: &str) -> Refusal {
    Effect::SessionPosition {
        message: format!(
            "{operation}: session directives are legal only at the REPL/CLI top level or the liminal space — not \
             nested in a query"
        ),
    }
    .into()
}

/// A session directive's arguments that do not fit its declared parameters.
pub(crate) fn session_arity(operation: &str, written: usize, required: usize, declared: usize) -> Refusal {
    delightql_types::diagnostic::DirectiveBinding::Arity {
        message: format!(
            "{operation} expects {} argument(s), got {written}",
            if required == declared { required.to_string() } else { format!("{required}..{declared}") }
        ),
    }
    .into()
}

/// A whole receipt piped where a session directive takes its arguments:
/// four declared columns into a parameter row of another width (THE
/// RETURNED PAYLOAD teaches the unwrap spelling).
pub(crate) fn receipt_shape(operation: &str, width: usize, params: usize) -> Refusal {
    Directive::ChainReceiptShape {
        message: format!(
            "a WHOLE receipt ({width} column(s)) is piped into {operation}, whose arguments are {params} \
             parameter(s): release the payload first with `|> .returned(*) |> {operation}(*)`"
        ),
    }
    .into()
}

/// An execution directive that is not the whole statement at the prompt
/// (FILES AND THE RUN: either execution form occupies the complete
/// statement).
pub(crate) fn run_position(operation: &str) -> Refusal {
    delightql_types::diagnostic::EffectRun::Position {
        message: format!(
            "{operation} starts a run and must be the entire statement at the REPL/CLI top level: a continuation \
             after it is not part of the run"
        ),
    }
    .into()
}

/// A namespace a run demands that declares no `main!`.
pub(crate) fn no_main(namespace: &str) -> Refusal {
    delightql_types::diagnostic::EffectRun::NoMain {
        message: format!(
            "namespace '{namespace}' has no main! to demand: consult a file that defines 'main!(*) :- …' into it first"
        ),
    }
    .into()
}

/// A run receipt read by a binding list of the wrong arity: glob access
/// releases the payload instead.
pub(crate) fn run_receipt_access(operation: &str, echo: &str) -> Refusal {
    delightql_types::diagnostic::EffectRun::ReceiptAccess {
        message: format!(
            "{operation}'s receipt heading is (success, operation, {echo}, returned) — the binding list is \
             exact-arity; glob access `(*)` releases the payload instead"
        ),
    }
    .into()
}

/// A selection whose deciding tier holds several identities: every
/// candidate is named, and none is chosen.
pub(crate) fn ambiguous_entity(name: &str, listed: &[String]) -> Refusal {
    Resolution::Ambiguous {
        message: format!(
            "Ambiguous entity '{name}': found in namespaces {}. Qualify the reference to choose one.",
            listed.join(", ")
        ),
    }
    .into()
}

/// A qualified mention routed into an archived blueprint: the archive is
/// inert (THE CROSSINGS: `imprint!` consumes its source).
pub(crate) fn blueprint_inert(archive: &str) -> Refusal {
    let target = archive.rsplit_once("::").map(|(parent, _)| parent).unwrap_or(archive);
    delightql_types::diagnostic::Blueprint::Inert {
        message: format!(
            "'{archive}' is an archived blueprint (imprint! consumed it into '{target}'); blueprints are \
             visible but inert — re-consult the source path for a live copy"
        ),
    }
    .into()
}

/// `retract!`'s argument that is not one whole-table designator.
pub(crate) fn retract_designator() -> Refusal {
    Directive::RetractMissing {
        message: "retract!'s target is a whole-table DESIGNATOR — `name(*)`, optionally namespace-qualified \
                  (`my::ns.name(*)`) — naming which definition to remove; filters, projections, and derived \
                  relations do not belong in a target"
            .to_string(),
    }
    .into()
}

/// `retract!` of a name the statement itself declares: the declaration is
/// the selected identity, and it is no session definition.
pub(crate) fn retract_claimed(spelled: &str, kind: &str, name: &str) -> Refusal {
    Directive::RetractOwnership {
        message: format!(
            "retract!({spelled}) selects the {kind} '{name}' this statement declares, which is not a session \
             definition; only a session-authored definition in `home` or a mutable scratch namespace may be \
             retracted"
        ),
    }
    .into()
}

/// `retract!` of a name nothing answers.
pub(crate) fn retract_missing(spelled: &str) -> Refusal {
    Directive::RetractMissing {
        message: format!("retract!({spelled}) selects nothing: no definition answers '{spelled}' at the prompt"),
    }
    .into()
}

/// `retract!` of a served identity: a table, a view, a predicate the
/// session did not author.
pub(crate) fn retract_served(spelled: &str, display: &str, kind: &str) -> Refusal {
    Directive::RetractOwnership {
        message: format!(
            "retract!({spelled}) selects {display} ({kind}) — a served entity the session did not author; only a \
             session-authored definition in `home` or a mutable scratch namespace may be retracted"
        ),
    }
    .into()
}

/// An imprinted entity read by another whose rule publishes a position with
/// no name: the object it becomes has no column a reader can name.
pub(crate) fn imprint_unnamed_read(name: &str, position: usize) -> Refusal {
    delightql_types::diagnostic::Manifest::SourceColumn {
        message: format!(
            "imprint!() entity '{name}' is read by another entity, but its rule publishes position {position} \
             with no name, so the object it becomes has no column a reader can name — name every published column"
        ),
    }
    .into()
}
