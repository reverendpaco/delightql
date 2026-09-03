// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Truth position — what accepts or rejects a tuple.
//!
//! ONE CARRIER. Every form that answers yes-or-no about a tuple is a
//! `TruthExpression`, in every position where one can stand. A truth reaches
//! value position only through the one directed crossing
//! (`FunctionApplication::Crossed`), after which it is an ordinary value; a
//! value never reaches truth position: a bare value where a predicate stands
//! has no derivation, so no variant here admits one.

use super::super::{Phase, Unresolved};
use super::chain::Chain;
use super::domain::DomainExpression;
use super::functions::PureCall;
use super::helpers::QualifiedName;
use crate::pipeline::asts::vocabulary::{Vec1, Vec2};
use crate::{lispy::ToLispy, ToLispy};
use delightql_types::SqlIdentifier;

/// `+` and `\+` — the mark that says which way a named proof is read.
///
/// DATA, one carrier, never a variant pair: the two spellings are one form
/// read two ways, so a consumer reads the polarity instead of matching a
/// second variant that repeats every other field.
///
/// POLARITY IS NOT OBSERVATION. `Positive` NAMES the proof and carries the
/// named truth's own answer, UNKNOWN included; `Negative` is DEFINED as the
/// two-valued "not proven TRUE", so it is already definite over every input,
/// the UNKNOWN-bodied rows included. What becomes of a positive proof belongs
/// to the position that consumes it (`TruthConsumer`, below): a query filter
/// observes it, and THERE the two polarities equipartition the input; a value
/// crossing and a database CHECK take the proof as it stands, so the
/// partition is the filter's, not the polarity's.
///
/// Inline `!( … )` is a different thing — Kleene NOT, which preserves
/// UNKNOWN — and it is `TruthExpression::Negation`, not a polarity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ToLispy)]
pub enum Polarity {
    #[lispy("polarity:positive")]
    Positive,
    #[lispy("polarity:negative")]
    Negative,
}

impl Polarity {
    /// Whether this polarity names the proof rather than its complement.
    pub fn is_positive(self) -> bool {
        matches!(self, Polarity::Positive)
    }

    /// The polarity a `+`/`\+` decision selects.
    pub fn from_positive(positive: bool) -> Self {
        if positive {
            Polarity::Positive
        } else {
            Polarity::Negative
        }
    }
}

/// WHO CONSUMES A TRUTH — the one judgment, made by the POSITION.
///
/// Polarity says which way a proof is read; this says what the position does
/// with the answer, and the two are distinct. Truth has exactly three
/// consumers and each has its own acceptance law:
///
/// | consumer | law | UNKNOWN |
/// |---|---|---|
/// | value crossing | preserve the truth value | carried as null |
/// | query filter | admit only TRUE | rejected |
/// | database CHECK | reject only FALSE | admitted |
///
/// Only a filter PARTITIONS its input, so only a filter collapses a positive
/// proof to a definite answer. A value carries the proof because the crossing
/// preserves the denotation; a CHECK carries it because SQL's own CHECK rule
/// already says what to do with UNKNOWN. Negative polarity is defined as "not
/// TRUE" and is already two-valued, so every consumer spells it the same way.
///
/// The consumer is named by the POSITION that consumes, once, at the entrance
/// — never rediscovered from the clause a lowering happened to emit into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruthConsumer {
    /// A value crossing: the truth becomes data and nothing observes it.
    Value,
    /// `WHERE`, `HAVING`, a filtering `ON`: the position admits only TRUE.
    Filter,
    /// A database CHECK: the row is refused exactly when the truth is FALSE.
    Constraint,
}

impl TruthConsumer {
    /// Whether a POSITIVE proof is collapsed to a definite answer here.
    ///
    /// The collapse is what makes the two polarities equipartition an input,
    /// and only a filter needs it. Wrapping a CHECK's positive proof in
    /// `IS TRUE` refuses every row the property is UNKNOWN about — the rows
    /// SQL's CHECK rule admits.
    pub fn observes_positive_proof(self) -> bool {
        matches!(self, TruthConsumer::Filter)
    }
}

/// A comparison of two VALUES. Both operands are domain expressions, so a
/// truth cannot stand in one: the operand type says so, and no consumer has
/// to re-check it.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("truth_expression:comparison")]
pub struct Comparison<P: Phase = Unresolved> {
    pub operator: crate::pipeline::asts::vocabulary::CmpOp,
    pub left: Box<DomainExpression<P>>,
    pub right: Box<DomainExpression<P>>,
}

/// What a membership tests: one value, or the row a comma makes.
///
/// A probe row is not a tuple VALUE written with brackets — it is truth
/// position's own row, and the two are different carriers because the
/// positions that admit them are different.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum Probe<P: Phase = Unresolved> {
    #[lispy("probe:value")]
    Value(Box<DomainExpression<P>>),
    /// `(x, y)` — the COMMA makes the row, so a row has at least two
    /// members by the grammar and by this type. One element parenthesized is
    /// a parenthesized operand, which normalizes to the bare value.
    #[lispy("probe:row")]
    Row(Vec2<DomainExpression<P>>),
}

impl<P: Phase> Probe<P> {
    /// The values this probe compares, in authored order. One for a bare
    /// value; the row's own members otherwise.
    /// The values this probe compares, in authored order.
    pub fn values(&self) -> Vec<&DomainExpression<P>> {
        match self {
            Probe::Value(value) => vec![value],
            Probe::Row(values) => values.iter().collect(),
        }
    }

    pub fn values_mut(&mut self) -> Vec<&mut DomainExpression<P>> {
        match self {
            Probe::Value(value) => vec![value.as_mut()],
            Probe::Row(row) => row.iter_mut().collect(),
        }
    }

    /// How wide the probe is. The membership's rows are checked against
    /// exactly this, once.
    pub fn width(&self) -> usize {
        self.values().len()
    }

    /// The probe's values, by value — what a lowering consumes. At least
    /// one: a bare value is one, and the comma that makes a row makes two.
    pub fn into_values(self) -> Vec1<DomainExpression<P>> {
        match self {
            Probe::Value(value) => Vec1::new(*value),
            Probe::Row(values) => {
                let (first, rest) = values.into_head_tail();
                Vec1::with_tail(first, rest)
            }
        }
    }

    /// The one value this probe tests, when it tests exactly one. A row
    /// probe answers `None` — a caller that wants a single column has not
    /// been handed one, and reading the row's first member would be a guess.
    pub fn sole_value(&self) -> Option<&DomainExpression<P>> {
        match self {
            Probe::Value(value) => Some(value),
            Probe::Row(_) => None,
        }
    }
}

/// One candidate row of a literal membership: `(1, 2)` in `(1, 2; 3, 4)`.
///
/// ROW STRUCTURE IS STRUCTURAL. A membership's candidates are rows, so they
/// are held as rows — flattening them into one list left the width of each
/// candidate to be re-derived downstream, and a multi-column probe had no
/// way to say which values belonged together.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("value_row")]
pub struct ValueRow<P: Phase = Unresolved>(pub Vec1<DomainExpression<P>>);

impl<P: Phase> ValueRow<P> {
    pub fn width(&self) -> usize {
        self.0.len()
    }

    pub fn values(&self) -> impl Iterator<Item = &DomainExpression<P>> {
        self.0.iter()
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut DomainExpression<P>> {
        self.0.iter_mut()
    }
}

/// `probe in (row; row)` — membership in an authored set of rows.
///
/// Membership negates with the KEYWORD `not`; polarity belongs to existence
/// and sigma alone, so the sigils and the keyword never trade places.
///
/// AT LEAST ONE CANDIDATE, structurally. Membership in nothing is not a
/// spelling the grammar admits, so it is not a value this type admits: a
/// lowering reduces the candidates without an empty fallback, and no
/// consumer has to assign a meaning to a set no author can write.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("truth_expression:membership")]
pub struct Membership<P: Phase = Unresolved> {
    pub probe: Probe<P>,
    pub negated: bool,
    pub rows: Vec1<ValueRow<P>>,
    pub source: MembershipSource,
}

/// The syntax that authored a membership predicate.
///
/// The witness spelling has stricter admission and diagnostic rules than
/// `in`, so the truth value retains that distinction after normalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ToLispy)]
pub enum MembershipSource {
    #[lispy("membership_source:in")]
    In,
    #[lispy("membership_source:witness_anon")]
    WitnessAnon,
}

/// What an authored truth PROBE said about the relation it probes: how the
/// author addressed it, and the dequalifying access that IS its correlation.
///
/// Spent at resolution — the probe's relation is resolved and the
/// correlation synthesized — so a resolved tree holds none of it. That is
/// what lets ONE existence carrier serve both phases instead of an
/// authored/resolved variant pair repeating every other field.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("probe_addressing")]
pub struct ProbeAddressing {
    pub identifier: QualifiedName,
    /// `+orders(*.(status))` — the dequalifying access inside the probe IS
    /// the correlation. Empty when explicit conditions are written instead.
    pub using_columns: Vec<SqlIdentifier>,
}

/// `probe in rel( … )` — membership in what a relation publishes.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("truth_expression:relational_membership")]
pub struct RelationalMembership<P: Phase = Unresolved> {
    pub probe: Probe<P>,
    pub negated: bool,
    pub relation: Box<Chain<P>>,
    /// Phase-selected: the authored addressing before resolution, nothing
    /// after. There is no resolved TWIN of this form — the phase changes
    /// what the field holds, not which variant exists.
    pub addressing: P::ProbeAddressing,
}

/// `+rel(, …)` / `\+rel(, …)` — existence, which IS truth.
///
/// One carrier in every home. In a comma continuation it restricts the
/// current relation; in a value position the crossing carries the same
/// node. A lowering may choose a semi- or antijoin, but that strategy is
/// not a second relational AST kind.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("truth_expression:existence")]
pub struct Existence<P: Phase = Unresolved> {
    pub polarity: Polarity,
    pub relation: Box<Chain<P>>,
    /// Phase-selected: the authored addressing before resolution, nothing
    /// after. ONE existence carrier in every phase.
    pub addressing: P::ProbeAddressing,
}

/// What a polarity OBSERVES: the named proof.
///
/// An authored application names a CALL, and nothing else — the body of a
/// DQL truth rule is fetched from the catalog where the name is resolved, so
/// `Body` is uninhabited before that and the authored arm cannot be built.
/// Resolution decides which: a bin predicate stays the atom its target
/// spells, and a rule becomes its body with the arguments in place.
///
/// The observation stays on the application either way. That is what carries
/// polarity to the lowering seam, where `IS TRUE` and `IS NOT TRUE` are
/// spelled — a collapse has no expression in truth position, so it cannot be
/// applied earlier without inventing a truth that is not one.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum NamedProof<P: Phase = Unresolved> {
    #[lispy("named_proof:call")]
    Call(PureCall<P>),
    #[lispy("named_proof:body")]
    Body(P::SigmaBody),
}

impl<P: Phase> NamedProof<P> {
    /// The call this proof is, when it is one.
    pub fn call(&self) -> Option<&PureCall<P>> {
        match self {
            NamedProof::Call(call) => Some(call),
            NamedProof::Body(_) => None,
        }
    }
}

/// `+like(a, "%x")` — the colon-less application.
///
/// Polarity is truth position's reinterpretation mark, as `:` is value
/// position's. ONE application carrier standing in truth position directly:
/// a sigma is not a second kind of expression that happens to contain a
/// truth, so nothing wraps it.
///
/// POLARITY NAMES WHICH ANSWER IS CARRIED. `+` carries the named proof's own
/// three-valued answer; `\+` is the definite "not proven TRUE" answer. The
/// consumer decides whether a positive proof is observed: only a filtering
/// position collapses it and thereby makes the two polarities equipartition
/// that position's input.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("truth_expression:sigma")]
pub struct SigmaApplication<P: Phase = Unresolved> {
    pub polarity: Polarity,
    pub proof: NamedProof<P>,
}

impl<P: Phase> SigmaApplication<P> {
    /// The authored form: a polarity and the call it observes.
    pub fn applied(polarity: Polarity, call: PureCall<P>) -> Self {
        SigmaApplication {
            polarity,
            proof: NamedProof::Call(call),
        }
    }
}

/// What an ARGUMENT supplies as a value, and the DISTINCT it asked for.
///
/// DISTINCT is argument DATA — the `%` before an argument modifies that
/// argument — so it lives here rather than in a domain variant any position
/// could have manufactured. The value is an ordinary domain expression: a
/// truth argument is the crossing standing in it, not a second arm here.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("argument_value")]
pub struct ArgumentValue<P: Phase = Unresolved> {
    /// `%expr` — the argument's values dedupe before the function sees
    /// them.
    pub distinct: bool,
    pub value: DomainExpression<P>,
}

impl<P: Phase> ArgumentValue<P> {
    /// An undecorated value argument.
    pub fn plain(value: DomainExpression<P>) -> Self {
        ArgumentValue {
            distinct: false,
            value,
        }
    }

    pub fn is_distinct(&self) -> bool {
        self.distinct
    }
}

/// Truth expressions — the one carrier for everything that accepts or
/// rejects a tuple.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum TruthExpression<P: Phase = Unresolved> {
    #[lispy("truth_expression:comparison")]
    Comparison(Comparison<P>),
    /// Boolean AND, N-ARY. Associativity makes binary nesting meaningless,
    /// so it is not carried: `a and b and c` is ONE conjunction of three,
    /// and the authored parenthesization is the CST's to remember. At least
    /// two members by the type, because one conjunct is that conjunct.
    ///
    /// The `Box` is the recursion break, not a design choice: a `Vec2`
    /// holds its first two members inline, so an unboxed one would make the
    /// enum contain itself.
    #[lispy("truth_expression:conjunction")]
    Conjunction(Box<Vec2<TruthExpression<P>>>),
    /// Boolean OR, n-ary for the same reason.
    #[lispy("truth_expression:disjunction")]
    Disjunction(Box<Vec2<TruthExpression<P>>>),
    /// `!( … )` — KLEENE NOT, which preserves UNKNOWN. Not a polarity: a
    /// polarity observes a named proof and collapses UNKNOWN, this does not.
    #[lispy("truth_expression:negation")]
    Not { expr: Box<TruthExpression<P>> },
    #[lispy("truth_expression:existence")]
    Existence(Existence<P>),
    #[lispy("truth_expression:membership")]
    Membership(Membership<P>),
    #[lispy("truth_expression:relational_membership")]
    RelationalMembership(RelationalMembership<P>),
    #[lispy("truth_expression:sigma")]
    Sigma(SigmaApplication<P>),
}

impl<P: Phase> TruthExpression<P> {
    /// Conjoin parts into ONE truth.
    ///
    /// One part IS that part — there is no one-member conjunction — and no
    /// parts is no truth, which the caller must have something to say
    /// about rather than receive an identity nobody wrote.
    ///
    /// A conjunction handed in is SPLICED: `and` is associative, so a
    /// conjunction of a conjunction is the same truth written with extra
    /// brackets, and this carrier holds one n-ary node. The opposite
    /// operator and a negation are ordinary members — neither is `and`, so
    /// neither flattens.
    pub fn all(parts: Vec<TruthExpression<P>>) -> Option<Self> {
        Self::combine(parts, Self::into_conjuncts, TruthExpression::Conjunction)
    }

    /// Disjoin parts into ONE truth, on the same terms — `or` splices `or`.
    pub fn any(parts: Vec<TruthExpression<P>>) -> Option<Self> {
        Self::combine(parts, Self::into_disjuncts, TruthExpression::Disjunction)
    }

    fn combine(
        parts: Vec<TruthExpression<P>>,
        splice: fn(Self, &mut Vec<Self>),
        n_ary: fn(Box<Vec2<Self>>) -> Self,
    ) -> Option<Self> {
        let mut members = Vec::with_capacity(parts.len());
        for part in parts {
            splice(part, &mut members);
        }
        match members.len() {
            0 => None,
            1 => members.pop(),
            _ => Vec2::try_from_vec(members).map(Box::new).map(n_ary),
        }
    }

    /// The conjuncts this truth states — the parts that must each hold on
    /// their own. A truth that is not a conjunction is its own sole
    /// conjunct.
    ///
    /// Only `and` splits: it is the one connective under which each part
    /// stands alone, so each can be owned separately. A disjunction is one
    /// condition.
    pub fn into_conjuncts(self, out: &mut Vec<TruthExpression<P>>) {
        match self {
            TruthExpression::Conjunction(parts) => {
                for part in (*parts).into_vec() {
                    part.into_conjuncts(out);
                }
            }
            other => out.push(other),
        }
    }

    /// The disjuncts this truth offers — the mirror of `into_conjuncts`, and
    /// `any`'s splice. It is not a splitter for consumers: a disjunction is
    /// ONE condition, so no owner can take one of its arms alone.
    fn into_disjuncts(self, out: &mut Vec<TruthExpression<P>>) {
        match self {
            TruthExpression::Disjunction(parts) => {
                for part in (*parts).into_vec() {
                    part.into_disjuncts(out);
                }
            }
            other => out.push(other),
        }
    }
}

impl<P: Phase> TruthExpression<P> {
    /// THE VALUES THIS TRUTH READS AT ITS OWN SCOPE, in authored order: a
    /// comparison's operands, a membership's probe and rows, a relational
    /// membership's probe, a sigma application's arguments — or, once the
    /// rule's body has been fetched, that body's own reads — through every
    /// connective and negation. A nested RELATION — an existence's or a
    /// relational membership's interior — is its own scope and is not
    /// entered: resolution, not this reading, says which of its names
    /// correlate outward.
    ///
    /// ONE STATEMENT of what a truth reads. A walk over VALUES reaches a
    /// crossed truth's reads through it the way it reaches an arithmetic
    /// operand, without enumerating truth's families itself. Whether a
    /// relation stands beneath any of those reads is the walk's judgment,
    /// `nests_relation`, asked of a value or a truth alike.
    pub fn scalar_operands(&self) -> Vec<&DomainExpression<P>> {
        let mut out = Vec::new();
        self.collect_scalar_operands(&mut out);
        out
    }

    /// The same reads, writable in place.
    pub fn scalar_operands_mut(&mut self) -> Vec<&mut DomainExpression<P>> {
        let mut out = Vec::new();
        self.collect_scalar_operands_mut(&mut out);
        out
    }

    fn collect_scalar_operands<'a>(&'a self, out: &mut Vec<&'a DomainExpression<P>>) {
        match self {
            TruthExpression::Comparison(Comparison { left, right, .. }) => {
                out.push(left);
                out.push(right);
            }
            TruthExpression::Conjunction(parts) | TruthExpression::Disjunction(parts) => {
                for part in parts.iter() {
                    part.collect_scalar_operands(out);
                }
            }
            TruthExpression::Not { expr } => expr.collect_scalar_operands(out),
            TruthExpression::Membership(Membership { probe, rows, .. }) => {
                out.extend(probe.values());
                for row in rows.iter() {
                    out.extend(row.values());
                }
            }
            TruthExpression::RelationalMembership(RelationalMembership { probe, .. }) => {
                out.extend(probe.values());
            }
            TruthExpression::Existence(_) => {}
            TruthExpression::Sigma(SigmaApplication { proof, .. }) => match proof {
                NamedProof::Call(call) => out.extend(call.call().arguments.value_domains()),
                NamedProof::Body(body) => P::sigma_body(body).collect_scalar_operands(out),
            },
        }
    }

    fn collect_scalar_operands_mut<'a>(&'a mut self, out: &mut Vec<&'a mut DomainExpression<P>>) {
        match self {
            TruthExpression::Comparison(Comparison { left, right, .. }) => {
                out.push(left.as_mut());
                out.push(right.as_mut());
            }
            TruthExpression::Conjunction(parts) | TruthExpression::Disjunction(parts) => {
                for part in parts.iter_mut() {
                    part.collect_scalar_operands_mut(out);
                }
            }
            TruthExpression::Not { expr } => expr.collect_scalar_operands_mut(out),
            TruthExpression::Membership(Membership { probe, rows, .. }) => {
                out.extend(probe.values_mut());
                for row in rows.iter_mut() {
                    out.extend(row.values_mut());
                }
            }
            TruthExpression::RelationalMembership(RelationalMembership { probe, .. }) => {
                out.extend(probe.values_mut());
            }
            TruthExpression::Existence(_) => {}
            TruthExpression::Sigma(SigmaApplication { proof, .. }) => match proof {
                NamedProof::Call(call) => out.extend(call.call_mut().arguments.value_domains_mut()),
                NamedProof::Body(body) => P::sigma_body_mut(body).collect_scalar_operands_mut(out),
            },
        }
    }
}
