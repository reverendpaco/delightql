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
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::{
    Comparison, Crossing, DomainExpression, Existence, FunctionApplication, Membership, Polarity,
    Probe, ProbeAddressing, RelationalMembership, SigmaApplication, TruthExpression, Unresolved,
    ValueRow, WholeHeading,
};
use crate::pipeline::asts::vocabulary::{Vec1, Vec2};
use crate::pipeline::syntax::{cst, TypedNode};

type Truth = TruthExpression<Unresolved>;

impl<'t> Normalizer<'t> {
    #[stacksafe::stacksafe]
    pub(crate) fn truth_expression(&mut self, node: cst::TruthExpression<'t>) -> Result<Truth> {
        match node {
            cst::TruthExpression::Comparison(comparison) => self.comparison(comparison),
            // POSITION OWNS ADMISSION. A whole-heading correlation names two
            // ARMS of a set operation and cannot be evaluated against one
            // row, so it is not a truth: the comma member on a set operation
            // admits it, and every truth position refuses it.
            cst::TruthExpression::HeadingCorrelation(_) => {
                Err(DelightQLError::validation_error_categorized(
                    "set/correlation/position",
                    "a whole-heading correlation relates two operands of a set operation, \
                     so it does not stand where a truth is read",
                    "write it as its own comma member on the set operation: \
                     `x(*) as a ; y(*) as b, a.* = b.*`",
                ))
            }
            cst::TruthExpression::ConjunctionExpression(conjunction) => {
                self.conjunction(conjunction)
            }
            cst::TruthExpression::DisjunctionExpression(disjunction) => {
                self.disjunction(disjunction)
            }
            cst::TruthExpression::Negation(negation) => {
                let inner = self.require(negation.child(), "a negation encloses a truth")?;
                Ok(TruthExpression::Not {
                    expr: Box::new(self.truth_expression(inner)?),
                })
            }
            // Parens are admission at truth level as at value level; the
            // truth they enclose is the truth.
            cst::TruthExpression::ParenthesizedTruth(parens) => {
                let inner = self.require(parens.child(), "parentheses enclose a truth")?;
                self.truth_expression(inner)
            }
            cst::TruthExpression::Membership(membership) => self.membership(membership),
            cst::TruthExpression::RelationalMembership(membership) => {
                self.relational_membership(membership)
            }
            cst::TruthExpression::Existence(existence) => self.existence(existence),
            cst::TruthExpression::SigmaApplication(application) => {
                self.sigma_application(application)
            }
        }
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
            DelightQLError::parse_error(format!("'{text}' is not a comparison operator"))
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
                    if let cst::ConjunctionExpressionChild::TruthExpression(truth) = child {
                        self.comma_truth_atoms(truth, wholes, terms)?;
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
            return Err(DelightQLError::validation_error_categorized(
                "set/correlation/operator",
                format!(
                    "a whole-heading correlation is written with '='; this one writes '{text}'"
                ),
                "within a correlation `=` is null-safe: a null meets a null",
            ));
        }
        let left = self.require(node.left(), "a correlation has a left operand")?;
        let right = self.require(node.right(), "a correlation has a right operand")?;
        let (left, left_positional) = self.heading_reference(left)?;
        let (right, right_positional) = self.heading_reference(right)?;
        // The two modes never mix: a step aligns by NAME or by POSITION, and
        // an atom that used both would name no alignment at all.
        if left_positional != right_positional {
            return Err(DelightQLError::validation_error_categorized(
                "set/correlation/mixed_modes",
                "a correlation aligns by NAME or by POSITION; this one writes both",
                "write `x.* = y.*` for the name modes, `x|*| = y|*|` for the positional one",
            ));
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
            return Err(DelightQLError::validation_error_categorized(
                "set/correlation/unnamed_arm",
                "a correlation operand names the arm it addresses; a bare glob names none",
                "write the arm: `x.* = y.*`",
            ));
        };
        Ok((self.qualifier(qualifier)?.spelling(), positional))
    }

    /// N-ary in the grammar and n-ary in the carrier: associativity makes
    /// nesting meaningless, so there is none to build.
    fn conjunction(&mut self, node: cst::ConjunctionExpression<'t>) -> Result<Truth> {
        let mut terms = Vec::new();
        for child in node.children() {
            match child {
                cst::ConjunctionExpressionChild::TruthExpression(truth) => {
                    terms.push(self.truth_expression(truth)?)
                }
                cst::ConjunctionExpressionChild::AndKeyword(_) => {}
            }
        }
        self.require(TruthExpression::all(terms), "a conjunction has a term")
    }

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
                        DelightQLError::parse_error("a membership row has a value")
                    })?;
                    rows.push(ValueRow(values));
                }
                cst::MembershipChild::InKeyword(_) => {}
            }
        }
        // A membership has at least one candidate row; the grammar says so
        // and the carrier says so, so the count is proved here and nothing
        // downstream reproves it or invents a meaning for nothing.
        let rows = Vec1::try_from_vec(rows)
            .ok_or_else(|| DelightQLError::parse_error("a membership has a candidate row"))?;
        Ok(TruthExpression::Membership(Membership {
            probe,
            rows,
            negated,
            source: crate::pipeline::asts::core::MembershipSource::In,
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
                addressing: ProbeAddressing {
                    identifier,
                    using_columns: Vec::new(),
                },
                negated,
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
                    || DelightQLError::parse_error("a probe row has at least two values"),
                )?))
            }
        }
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
        let subquery = self.interior_relation(callee, ho_part, interior)?;
        // A dequalifying access inside the probe IS the USING correlation:
        // `+orders(*.(status))` names the shared columns, and the access the
        // mention already carries is where they were decided.
        let using_columns = match (subquery.as_read_relation(), subquery.head_access()) {
            (
                Some(crate::pipeline::asts::core::Relation::Ground { .. }),
                Some(crate::pipeline::asts::core::Access::Dequalify(columns)),
            ) => columns.clone(),
            _ => Vec::new(),
        };
        Ok(TruthExpression::Existence(Existence {
            polarity,
            relation: Box::new(subquery),
            addressing: ProbeAddressing {
                identifier,
                using_columns,
            },
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
            other => Err(DelightQLError::parse_error(format!(
                "'{other}' is not a polarity"
            ))),
        }
    }
}
