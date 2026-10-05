// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Truth position — what accepts or rejects a tuple.
//!
//! **EXISTENCE IS TRUTH.** `+rel(, …)` and `\+rel(, …)` have ONE
//! truth-expression carrier. In a comma continuation that truth restricts the
//! current relation; an implementation may lower it as a semi- or antijoin,
//! but that SQL strategy is not a second relational AST kind, and nothing here
//! produces one. Value-position existence reaches the same carrier through the
//! truth-to-value crossing.
//!
//! **CROSSING LAW — one carrier, one direction.** A truth enters value
//! position wherever a value stands, through the one crossing minted here;
//! value never enters truth position: a bare value where a predicate stands
//! has no derivation, so nothing here has to refuse one.

use super::{value::comparison_operator, Normalizer};
use crate::diagnostic::{Internal, Parse, Set};
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::{
    Comparison, Crossing, DomainExpression, Existence, FunctionApplication, Membership, Polarity,
    Probe, ProbeAddressing, RelationalMembership, SigmaApplication, TruthExpression, Unresolved,
    ValueRow, WholeHeading,
};
use crate::pipeline::asts::vocabulary::{Vec1, Vec2};
use crate::pipeline::syntax::{cst, TypedNode};

type Truth = TruthExpression<Unresolved>;

/// POSITION OWNS ADMISSION. A whole-heading correlation names two ARMS of a
/// set operation and cannot be evaluated against one row, so it is not a
/// truth: the comma member on a set operation admits it, and every truth
/// position refuses it.
fn correlation_position() -> DelightQLError {
    DelightQLError::from(Set::CorrelationPosition {
        message: "a whole-heading correlation relates two operands of a set operation, \
                  so it does not stand where a truth is read"
            .to_string(),
    })
}

impl<'t> Normalizer<'t> {
    #[stacksafe::stacksafe]
    pub(crate) fn truth_expression(&mut self, node: cst::TruthExpression<'t>) -> Result<Truth> {
        match node {
            cst::TruthExpression::Comparison(comparison) => self.comparison(comparison),
            cst::TruthExpression::HeadingCorrelation(_) => Err(correlation_position()),
            cst::TruthExpression::ConjunctionExpression(conjunction) => {
                self.conjunction(conjunction)
            }
            cst::TruthExpression::DisjunctionExpression(disjunction) => {
                self.disjunction(disjunction)
            }
            cst::TruthExpression::Negation(negation) => self.negation(negation),
            cst::TruthExpression::ParenthesizedTruth(parens) => self.parenthesized_truth(parens),
            cst::TruthExpression::Membership(membership) => self.membership(membership),
            cst::TruthExpression::RelationalMembership(membership) => {
                self.relational_membership(membership)
            }
            cst::TruthExpression::Existence(existence) => self.existence(existence),
            cst::TruthExpression::ExistsAnonGrelex(existence) => {
                self.anonymous_existence(existence)
            }
            cst::TruthExpression::SigmaApplication(application) => {
                self.sigma_application(application)
            }
            cst::TruthExpression::MixedConnectiveRun(run) => Err(self.mixed_connective_run(run)),
        }
    }

    /// THE MIXTURE IS RECOGNIZED TO BE REFUSED. The grammar admits a run that
    /// mixes `and` and `or` only as this witness, so the refusal can name the
    /// two words the author wrote, in their order, wherever the run stands —
    /// and nothing of the run is ever normalized.
    fn mixed_connective_run(&self, node: cst::MixedConnectiveRun<'t>) -> DelightQLError {
        use cst::MixedConnectiveRunChild as Child;
        let mut words: Vec<&str> = Vec::new();
        for child in node.children() {
            let word = match child {
                Child::AndKeyword(word) => self.text(word),
                Child::OrKeyword(word) => self.text(word),
                Child::Comparison(_)
                | Child::HeadingCorrelation(_)
                | Child::Membership(_)
                | Child::RelationalMembership(_)
                | Child::Negation(_)
                | Child::Existence(_)
                | Child::ExistsAnonGrelex(_)
                | Child::SigmaApplication(_)
                | Child::ParenthesizedTruth(_) => continue,
            };
            if !words.iter().any(|seen| seen.eq_ignore_ascii_case(word)) {
                words.push(word);
            }
        }
        let (first, second) = match words.as_slice() {
            [first, second, ..] => (*first, *second),
            _ => {
                return Internal::invariant(
                    "normalize::truth",
                    "a mixed connective run carries both connectives",
                )
            }
        };
        DelightQLError::from(Parse::Pony {
            message: format!(
                "mixed connectives `{first}` and `{second}` without grouping: \
                 DelightQL has NO precedence between `and` and `or` (no PEMDAS), \
                 so the truth has no reading. Parenthesize the composition: \
                 `((a {first} b) {second} c)` or `(a {first} (b {second} c))`."
            ),
        })
    }

    /// One child of a conjunction: an operand read through the same per-kind
    /// reader `truth_expression` uses, or `None` for a separator. A
    /// connective's operand is every truth form but a connective — the
    /// grammar keeps the two connectives from meeting ungrouped, so nothing
    /// here has to — and the match is exhaustive so a member the grammar adds
    /// is a member this reader must place.
    #[stacksafe::stacksafe]
    fn conjunct(&mut self, child: cst::ConjunctionExpressionChild<'t>) -> Result<Option<Truth>> {
        use cst::ConjunctionExpressionChild as Child;
        Ok(Some(match child {
            Child::AndKeyword(_) | Child::CommaSigil(_) => return Ok(None),
            Child::Comparison(comparison) => self.comparison(comparison)?,
            Child::HeadingCorrelation(_) => return Err(correlation_position()),
            Child::Negation(negation) => self.negation(negation)?,
            Child::ParenthesizedTruth(parens) => self.parenthesized_truth(parens)?,
            Child::Membership(membership) => self.membership(membership)?,
            Child::RelationalMembership(membership) => self.relational_membership(membership)?,
            Child::Existence(existence) => self.existence(existence)?,
            Child::ExistsAnonGrelex(existence) => self.anonymous_existence(existence)?,
            Child::SigmaApplication(application) => self.sigma_application(application)?,
        }))
    }

    fn negation(&mut self, node: cst::Negation<'t>) -> Result<Truth> {
        let inner = self.require(node.child(), "a negation encloses a truth")?;
        Ok(TruthExpression::Not {
            expr: Box::new(self.truth_expression(inner)?),
        })
    }

    /// Parens are admission at truth level as at value level; the truth they
    /// enclose is the truth.
    fn parenthesized_truth(&mut self, node: cst::ParenthesizedTruth<'t>) -> Result<Truth> {
        let inner = self.require(node.child(), "parentheses enclose a truth")?;
        self.truth_expression(inner)
    }

    /// THE ONE MINT. A truth written where a value stands is read as the
    /// truth it is and crossed HERE, once, into the ordinary value family.
    /// Every value position reaches this through the value spine, so no
    /// position has a crossing of its own and nothing downstream decides
    /// that a truth is a value.
    pub(crate) fn crossed_truth(
        &mut self,
        node: cst::CrossedTruth<'t>,
    ) -> Result<DomainExpression<Unresolved>> {
        let inner = self.require(node.child(), "the crossing carries a truth")?;
        let truth = self.require(
            <cst::TruthExpression<'t> as TypedNode<'t>>::cast(inner.node()),
            "the crossing carries a truth",
        )?;
        let truth = self.truth_expression(truth)?;
        Ok(DomainExpression::Application(FunctionApplication::Crossed(
            Crossing::originate(super::CrossingPermit::grant(), truth),
        )))
    }

    fn comparison(&mut self, node: cst::Comparison<'t>) -> Result<Truth> {
        let mut operands = Vec::new();
        let mut operator = None;
        for child in node.children() {
            match child {
                cst::ComparisonChild::Operand(operand) => operands.push(operand),
                cst::ComparisonChild::CmpOp(op) => operator = Some(op),
            }
        }
        let operator = self.require(operator, "a comparison has an operator")?;
        let text = self.text(operator);
        let operator = comparison_operator(text).ok_or_else(|| {
            Internal::invariant(
                "normalize::truth",
                format!("'{text}' is not a comparison operator"),
            )
        })?;
        let mut operands = operands.into_iter();
        let left = self.require(operands.next(), "a comparison has a left operand")?;
        let right = self.require(operands.next(), "a comparison has a right operand")?;
        let left = self.operand(left)?;
        let right = self.operand(right)?;
        Ok(TruthExpression::Comparison(Comparison {
            operator,
            left: Box::new(left),
            right: Box::new(right),
        }))
    }

    /// The whole-heading correlations a comma member writes at its top
    /// level, and the truth that remains.
    ///
    /// `and` at the top of a comma member is the same thing as writing two
    /// comma members, so a correlation conjoined with a predicate splits
    /// into the two continuations it means. Nesting one under `or`, `!`, or
    /// any other truth reaches `truth_expression`, which refuses it.
    pub(crate) fn comma_truth(
        &mut self,
        node: cst::TruthExpression<'t>,
    ) -> Result<(Vec<WholeHeading<Unresolved>>, Option<Truth>)> {
        let mut wholes = Vec::new();
        let mut terms = Vec::new();
        self.comma_truth_atoms(node, &mut wholes, &mut terms)?;
        Ok((wholes, TruthExpression::all(terms)))
    }

    #[stacksafe::stacksafe]
    fn comma_truth_atoms(
        &mut self,
        node: cst::TruthExpression<'t>,
        wholes: &mut Vec<WholeHeading<Unresolved>>,
        terms: &mut Vec<Truth>,
    ) -> Result<()> {
        match node {
            cst::TruthExpression::HeadingCorrelation(correlation) => {
                wholes.push(self.heading_correlation(correlation)?);
            }
            cst::TruthExpression::ConjunctionExpression(conjunction) => {
                for child in conjunction.children() {
                    match child {
                        cst::ConjunctionExpressionChild::HeadingCorrelation(correlation) => {
                            wholes.push(self.heading_correlation(correlation)?);
                        }
                        // Parens keep a correlation at the comma's top level.
                        cst::ConjunctionExpressionChild::ParenthesizedTruth(parens) => {
                            let inner =
                                self.require(parens.child(), "parentheses enclose a truth")?;
                            self.comma_truth_atoms(inner, wholes, terms)?;
                        }
                        other => terms.extend(self.conjunct(other)?),
                    }
                }
            }
            // Parens are admission, here as everywhere: the truth they
            // enclose is the truth, and a correlation inside them is still
            // written at the comma's top level.
            cst::TruthExpression::ParenthesizedTruth(parens) => {
                let inner = self.require(parens.child(), "parentheses enclose a truth")?;
                self.comma_truth_atoms(inner, wholes, terms)?;
            }
            other => terms.push(self.truth_expression(other)?),
        }
        Ok(())
    }

    /// THE WHOLE HEADING CORRELATES, in the mode the step aligns by: `x.* =
    /// y.*` names every name both arms publish, `x|*| = y|*|` every position.
    /// Both operands name a STAGE — a bare glob names none — and the two
    /// modes are two forms because the columns they pair are found two
    /// different ways.
    fn heading_correlation(
        &mut self,
        node: cst::HeadingCorrelation<'t>,
    ) -> Result<WholeHeading<Unresolved>> {
        let operator = self.require(node.operator(), "a correlation has an operator")?;
        let text = self.text(operator);
        // Within a correlation `=` is null-safe, and it is the ONE spelling:
        // a correlation is not an ordinary comparison wearing globs.
        if !matches!(text, "=") {
            return Err(DelightQLError::from(Set::CorrelationOperator {
                message: format!(
                    "a whole-heading correlation is written with '='; this one writes '{text}'"
                ),
            }));
        }
        let left = self.require(node.left(), "a correlation has a left operand")?;
        let right = self.require(node.right(), "a correlation has a right operand")?;
        let (left, left_positional) = self.heading_reference(left)?;
        let (right, right_positional) = self.heading_reference(right)?;
        // The two modes never mix: a step aligns by NAME or by POSITION, and
        // an atom that used both would name no alignment at all.
        if left_positional != right_positional {
            return Err(DelightQLError::from(Set::CorrelationMixedModes {
                message: "a correlation aligns by NAME or by POSITION; this one writes both"
                    .to_string(),
            }));
        }
        Ok(if left_positional {
            WholeHeading::ByPosition { left, right }
        } else {
            WholeHeading::ByName { left, right }
        })
    }

    /// The stage a correlation operand names, and whether it names it
    /// positionally.
    fn heading_reference(
        &mut self,
        node: cst::HeadingReference<'t>,
    ) -> Result<(delightql_types::SqlIdentifier, bool)> {
        let (qualifier, positional) = match node {
            cst::HeadingReference::Glob(glob) => (glob.qualifier(), false),
            cst::HeadingReference::PositionalHeading(heading) => (heading.qualifier(), true),
        };
        let Some(qualifier) = qualifier else {
            return Err(DelightQLError::from(Set::CorrelationUnnamedArm {
                message: "a correlation operand names the arm it addresses; a bare glob names none"
                    .to_string(),
            }));
        };
        Ok((self.qualifier(qualifier)?.spelling(), positional))
    }

    /// N-ary in the grammar and n-ary in the carrier: associativity makes
    /// nesting meaningless, so there is none to build. The separators are
    /// two spellings of one conjunction — `and` everywhere, the comma where a
    /// truth stands alone — and the carrier records neither.
    fn conjunction(&mut self, node: cst::ConjunctionExpression<'t>) -> Result<Truth> {
        let mut terms = Vec::new();
        for child in node.children() {
            terms.extend(self.conjunct(child)?);
        }
        self.require(TruthExpression::all(terms), "a conjunction has a term")
    }

    /// The `or` run takes connective operands and the `;` form takes whole
    /// truths inside its own parens; the grammar sorted them, and both arrive
    /// as truths.
    fn disjunction(&mut self, node: cst::DisjunctionExpression<'t>) -> Result<Truth> {
        let mut terms = Vec::new();
        for child in node.children() {
            match child {
                cst::DisjunctionExpressionChild::TruthExpression(truth) => {
                    terms.push(self.truth_expression(truth)?)
                }
                cst::DisjunctionExpressionChild::OrKeyword(_)
                | cst::DisjunctionExpressionChild::CorrespondingUnionSigil(_) => {}
            }
        }
        self.require(TruthExpression::any(terms), "a disjunction has a term")
    }

    /// Membership negates with the KEYWORD; the sigils and the keyword never
    /// trade places, so `not` is read here and polarity is not.
    fn membership(&mut self, node: cst::Membership<'t>) -> Result<Truth> {
        let probe = self.require(node.probe(), "a membership has a probe")?;
        let probe = self.probe(probe)?;
        let mut negated = false;
        let mut rows = Vec::new();
        for child in node.children() {
            match child {
                cst::MembershipChild::NotKeyword(_) => negated = true,
                // A ROW IS A ROW. Each `value_row` becomes one candidate,
                // so a multi-column probe keeps knowing which values belong
                // together; flattening every row into one list left the
                // candidate width to be guessed downstream.
                cst::MembershipChild::ValueRow(row) => {
                    let mut values = Vec::new();
                    for member in row.children() {
                        match member {
                            cst::ValueRowChild::DomainExpression(expression) => {
                                values.push(self.domain_expression(expression)?)
                            }
                            cst::ValueRowChild::CommaSigil(_) => {}
                        }
                    }
                    // A value_row has at least one value; the grammar says
                    // so and the carrier says so.
                    let values = Vec1::try_from_vec(values).ok_or_else(|| {
                        Internal::invariant("normalize::truth", "a membership row has a value")
                    })?;
                    rows.push(ValueRow(values));
                }
                cst::MembershipChild::InKeyword(_) => {}
            }
        }
        // A membership has at least one candidate row; the grammar says so
        // and the carrier says so, so the count is proved here and nothing
        // downstream reproves it or invents a meaning for nothing.
        let rows = Vec1::try_from_vec(rows).ok_or_else(|| {
            Internal::invariant("normalize::truth", "a membership has a candidate row")
        })?;
        Ok(TruthExpression::Membership(Membership {
            probe,
            rows,
            negated,
            source: crate::pipeline::asts::core::MembershipSource::In,
            matching: (),
        }))
    }

    fn relational_membership(&mut self, node: cst::RelationalMembership<'t>) -> Result<Truth> {
        let probe = self.require(node.probe(), "a membership has a probe")?;
        let probe = self.probe(probe)?;
        let callee = self.require(node.callee(), "a relational membership names a relation")?;
        let (identifier, _) = self.relation_identifier(callee)?;
        let interior = self.require(node.interior(), "a relational membership has an interior")?;
        let subquery = self.interior_relation(callee, None, interior)?;
        let negated = node
            .children()
            .any(|child| matches!(child, cst::RelationalMembershipChild::NotKeyword(_)));
        Ok(TruthExpression::RelationalMembership(
            RelationalMembership {
                probe,
                relation: Box::new(subquery),
                addressing: ProbeAddressing { identifier },
                negated,
                matching: (),
            },
        ))
    }

    /// ONE element is a parenthesized operand; the COMMA makes the row.
    ///
    /// A probe row is truth position's own row, not a tuple VALUE written
    /// with brackets — the two are different carriers because the positions
    /// admitting them are different.
    fn probe(&mut self, node: cst::Probe<'t>) -> Result<Probe<Unresolved>> {
        match node {
            cst::Probe::DomainExpression(expression) => {
                Ok(Probe::Value(Box::new(self.domain_expression(expression)?)))
            }
            cst::Probe::ProbeRow(row) => {
                let mut elements = Vec::new();
                for child in row.children() {
                    match child {
                        cst::ProbeRowChild::DomainExpression(expression) => {
                            elements.push(self.domain_expression(expression)?)
                        }
                        cst::ProbeRowChild::CommaSigil(_) => {}
                    }
                }
                // THE COMMA MAKES THE ROW: one parenthesized element is a
                // parenthesized operand, which normalized to the bare value.
                Ok(Probe::Row(Vec2::try_from_vec(elements).ok_or_else(
                    || {
                        Internal::invariant(
                            "normalize::truth",
                            "a probe row has at least two values",
                        )
                    },
                )?))
            }
        }
    }

    /// Anonymous existence is the anonymous-table spelling of membership.
    /// Every truth position and the comma continuation reach this one
    /// construction, so consulted bodies cannot acquire a private evaluator.
    pub(crate) fn anonymous_existence(&mut self, node: cst::ExistsAnonGrelex<'t>) -> Result<Truth> {
        let mut body = None;
        let mut opener = None;
        for child in node.children() {
            match child {
                cst::ExistsAnonGrelexChild::AnonBody(node) => body = Some(node),
                cst::ExistsAnonGrelexChild::ExistsAnonOpen(node) => opener = Some(node),
            }
        }
        let table = self.anon_body(self.require(body, "an anonymous membership has a body")?)?;
        let opener = self.require(opener, "an anonymous membership carries polarity")?;
        let header = table.body.header.ok_or_else(|| {
            DelightQLError::from(crate::diagnostic::AnonBinding::WitnessShape {
                message: "a witness anonymous table is a membership test and needs headers"
                    .to_string(),
            })
        })?;
        let mut probes = header
            .into_vec()
            .into_iter()
            .map(|item| {
                item.slot.into_term().ok_or_else(|| {
                    Internal::invariant(
                        "normalize::truth",
                        "an anonymous membership header has a value",
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let probe = if probes.len() == 1 {
            Probe::Value(Box::new(probes.pop().expect("one probe")))
        } else {
            Probe::Row(Vec2::try_from_vec(probes).ok_or_else(|| {
                Internal::invariant("normalize::truth", "an anonymous membership has a probe")
            })?)
        };
        let rows = table
            .body
            .rows
            .map(|row| ValueRow((*row.0).map(crate::pipeline::asts::core::Datum::into_value)));
        Ok(TruthExpression::Membership(Membership {
            probe,
            negated: self.text(opener).starts_with('\\'),
            rows,
            source: crate::pipeline::asts::core::MembershipSource::WitnessAnon,
            matching: (),
        }))
    }

    /// The ONE existence carrier. Both the truth-position spelling and the
    /// value-position one reach here; the difference is which position asked,
    /// and the position is what the surrounding node already decided.
    pub(crate) fn existence(&mut self, node: cst::Existence<'t>) -> Result<Truth> {
        let polarity = self.require(node.child(), "existence carries a polarity")?;
        let polarity = self.polarity(polarity)?;
        let callee = self.require(node.callee(), "existence names a relation")?;
        let interior = self.require(node.interior(), "existence has an interior")?;
        self.existence_carrier(callee, node.ho_part(), interior, polarity)
    }

    fn existence_carrier(
        &mut self,
        callee: cst::RelationName<'t>,
        ho_part: Option<cst::HoPart<'t>>,
        interior: cst::InteriorContinuation<'t>,
        polarity: Polarity,
    ) -> Result<Truth> {
        let (identifier, _) = self.relation_identifier(callee)?;
        // A dequalifying access inside the probe — `+orders(*.(status))` —
        // is the read's own correlation to the row the probe stands in;
        // the access stays on the mention that carries it.
        let subquery = self.interior_relation(callee, ho_part, interior)?;
        Ok(TruthExpression::Existence(Existence {
            polarity,
            relation: Box::new(subquery),
            addressing: ProbeAddressing { identifier },
        }))
    }

    /// Colon-less: polarity is truth position's reinterpretation mark, as `:`
    /// is value position's. ONE application carrier after build.
    fn sigma_application(&mut self, node: cst::SigmaApplication<'t>) -> Result<Truth> {
        let callee = self.require(node.callee(), "a sigma application names a predicate")?;
        let callee = self.require(callee.child(), "a callee is a predicate identifier")?;
        let reference = self.plain_reference(callee)?;

        let mut polarity = None;
        let mut arguments = Vec::new();
        for child in node.children() {
            match child {
                cst::SigmaApplicationChild::Polarity(mark) => polarity = Some(self.polarity(mark)?),
                cst::SigmaApplicationChild::Argument(argument) => {
                    arguments.push(self.argument(argument)?)
                }
                cst::SigmaApplicationChild::CommaSigil(_) => {}
            }
        }
        let polarity = self.require(polarity, "a sigma application carries a polarity")?;
        let call =
            crate::pipeline::asts::core::FunctorCall::scalar_application(reference, arguments);
        Ok(TruthExpression::Sigma(SigmaApplication::applied(
            polarity,
            self.seal_pure(call)?,
        )))
    }

    /// `+` and `\+` are DATA, one carrier — never a variant pair. The token's
    /// bytes ARE the datum, decoded once here.
    pub(crate) fn polarity(&self, node: cst::Polarity<'t>) -> Result<Polarity> {
        match self.text(node) {
            "+" => Ok(Polarity::Positive),
            "\\+" => Ok(Polarity::Negative),
            other => Err(Internal::invariant(
                "normalize::truth",
                format!("'{other}' is not a polarity"),
            )),
        }
    }
}
