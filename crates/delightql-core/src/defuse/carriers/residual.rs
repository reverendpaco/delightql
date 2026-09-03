// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A RESIDUAL'S PREFIX AND CAPTURE, and an effect's residual over its
//! scratch: the two carrier constructions of a closed residual, as
//! operations of the carrier authority. The evaluation row is taken here,
//! the configured values resolve over it here, the landing that realizes
//! the row with the values beside it is bound here together with the
//! occurrence it stands in place of and the token it publishes, and the
//! value carrier follows it. No caller states any of those facts.

use super::{CarrierRecord, CompilerRow, ResidualCaptureSource};
use crate::defuse::ho::RuleValueId;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::{AuthoredColumn, ColumnOccurrence, NamedReference, Reference};
use crate::pipeline::query_features::HoParamBindings;
use crate::pipeline::resolver::ResolvedRelation;
use delightql_types::SqlIdentifier;

/// What a residual's construction captured: the construction token its
/// landing publishes and the ports that cross into the receiving use.
#[derive(Clone)]
pub(in crate::defuse) struct ResidualCapture {
    pub(in crate::defuse) row_token: crate::relation::PortId,
    pub(in crate::defuse) crossing: Vec<crate::relation::PortId>,
}

/// THE CONSTRUCTION WORLD AT A RESIDUAL DESIGNATOR, TOTAL.
///
/// A configured expression that reads caller data is evaluated over exactly
/// one relation, and this says which. Where that relation is the caller row
/// itself, the world HOLDS that row's own carrier: the identity
/// construction reads and the carrier it spends are one value, so no
/// identity can name a row the site does not hold, nothing can be present
/// after its other half was spent, and there is no agreement left to check.
pub(in crate::defuse) enum ResidualEvaluationRow<'row> {
    /// THE CALLER ROW AT THIS CALL SITE — standing, already absorbed, or
    /// never there. Ground and row-free values remain lawful with no row;
    /// a caller-dependent computation does not. Construction spends the row
    /// through this borrow, which is the same act that tells ordinary join
    /// assembly the row is gone.
    Caller(&'row mut crate::pipeline::resolver::CallerRow),
    /// A relation already realized as a carrier or plan scratch: a prepared
    /// pipe landing, a capture a preceding sibling already crossed, or an
    /// effect invocation's own evaluation row. Reading it spends nothing.
    Realized(ResidualCaptureSource),
}

impl ResidualEvaluationRow<'_> {
    /// The same world, borrowed for one nested act. A realized relation is
    /// a fact anyone may read; the caller row is re-lent, so a nested
    /// construction that spends it spends the one row.
    pub(in crate::defuse) fn reborrow(&mut self) -> ResidualEvaluationRow<'_> {
        match self {
            ResidualEvaluationRow::Caller(row) => ResidualEvaluationRow::Caller(row),
            ResidualEvaluationRow::Realized(source) => {
                ResidualEvaluationRow::Realized(source.clone())
            }
        }
    }
}

/// A residual's prefix as this authority prepared it: its actuals, whose
/// carriers are the record this authority bound, and the capture it made.
pub(in crate::defuse) struct PreparedResidualPrefix {
    pub(in crate::defuse) actuals: crate::defuse::bound_use::HoActuals,
    pub(in crate::defuse) capture: Option<ResidualCapture>,
}

/// PREPARE A RESIDUAL'S PREFIX: its relation actuals bound as carriers,
/// its scalar actuals resolved — a correlated actual over the evaluation
/// row this authority takes here, then captured beside that row as one
/// materialized carrier — and its staged scalars bound. The one operation
/// through which a residual acquires carriers.
pub(in crate::defuse) fn prepare_residual_prefix(
    mut bindings: HoParamBindings,
    scalar_actuals: Vec<(String, ast_unresolved::DomainExpression)>,
    rules: std::collections::HashMap<SqlIdentifier, RuleValueId>,
    prepared: Option<CarrierRecord>,
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold<'_, '_>,
    evaluation_row: ResidualEvaluationRow<'_>,
) -> Result<PreparedResidualPrefix> {
    let mut authored_bare = std::collections::HashMap::new();
    for (param, expr) in &scalar_actuals {
        if let ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
            AuthoredColumn {
                name,
                qualifier: None,
                ..
            },
        ))) = expr
        {
            authored_bare.insert(param.clone(), name.to_string());
        }
    }
    let interior = std::mem::take(&mut bindings.interior_ctes);
    let mut carriers = super::call::resolve_carriers(fold, &mut bindings, None, interior, None)?;
    if let Some(mut prepared) = prepared {
        prepared.absorb(carriers);
        carriers = prepared;
    }
    let mut values = std::collections::HashMap::new();
    let mut capture = None;
    if !scalar_actuals.is_empty() {
        // A CORRELATED ACTUAL SPENDS THE EVALUATION ROW. Whether one is
        // present is a fact of the authored expressions, so the row is
        // taken HERE — from the fold's own position, before the child that
        // resolves the actuals borrows it — and handed to the capture below.
        // A realized relation is read through a fresh ground read and
        // spends nothing; the caller row is TAKEN, and taking it is what
        // records that ordinary join assembly must not spend it too. Its
        // identity is the carrier's own, so there is nothing here to agree
        // with.
        let reads_a_row = scalar_actuals.iter().any(|(_, expr)| {
            !matches!(
                expr,
                ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(_)
                )
            ) && !is_bare_row_reference(expr)
                && domain_reads_a_row(expr)
        });
        // A REALIZED ROW NOBODY SPENDS STILL STANDS IN VIEW: a named
        // actual keeps its occurrence, and the occurrence it keeps is the
        // realized relation's own port. A caller row stands as the frame
        // the enclosing position already holds, so nothing enters it twice.
        let realized_in_view = match (&evaluation_row, reads_a_row) {
            (ResidualEvaluationRow::Realized(source), false) => Some(source.row),
            _ => None,
        };
        let taken_row = if reads_a_row {
            Some(match evaluation_row {
                ResidualEvaluationRow::Realized(source) => (
                    ResolvedRelation::over(source.row, &fold.core.identities)?,
                    source.row.relation(),
                    source.leading_ctes,
                    source.absorbs_join_input,
                ),
                ResidualEvaluationRow::Caller(row) => {
                    let Some(row) = row.absorb(&mut fold.lexical) else {
                        return Err(DelightQLError::validation_error_categorized(
                            "resolution/ho/residual-capture",
                            "a configured rule-value expression reads caller data, but this construction position has no caller relation",
                            "construct the value where its caller row stands, or configure it with a row-free expression",
                        ));
                    };
                    // THE IDENTITY IS THE CARRIER'S OWN. Nothing
                    // supplied it beside the row, so nothing can
                    // have named a different one.
                    let occurrence = row.semantic_relation();
                    (row, occurrence, Vec::new(), true)
                }
            })
        } else {
            None
        };
        let mut actual_fold = fold.child();
        // THE TAKEN ROW STAYS IN VIEW while the actuals resolve — it is the
        // row they read — and comes back out below for the capture.
        let mut taken_row = taken_row;
        let mut row_in_frame = match taken_row.take() {
            Some((row, source_relation, leading, absorbs)) => {
                actual_fold
                    .lexical
                    .enter(row, crate::pipeline::resolver::Reach::Row);
                Some((source_relation, leading, absorbs))
            }
            None => None,
        };
        if let Some(row) = realized_in_view {
            let realized = ResolvedRelation::over(row, &actual_fold.core.identities)?;
            actual_fold
                .lexical
                .enter(realized, crate::pipeline::resolver::Reach::Row);
        }
        // THE CARRIERS ARE THE ROW THE ACTUALS STAND OVER, enclosed by the
        // evaluation row and the caller's position: a carrier's name
        // shadows the outer occurrence of the same name; other outer
        // columns stay reachable. The row is minted from the record's own
        // receipts.
        let carriers_framed = actual_fold
            .lexical
            .enter_carriers(&carriers, &actual_fold.core.identities)?;
        let mut staged = Vec::new();
        let mut correlated = Vec::new();
        for (param, expr) in scalar_actuals {
            match expr {
                ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(value),
                ) => {
                    bindings
                        .scalar_literals
                        .entry(param.clone())
                        .or_insert_with(|| value.clone());
                    values.insert(
                        SqlIdentifier::new(param),
                        crate::pipeline::asts::resolved::DomainExpression::Application(
                            crate::pipeline::asts::resolved::FunctionApplication::Ground(value),
                        ),
                    );
                }
                expr if is_bare_row_reference(&expr) => {
                    values.insert(
                        SqlIdentifier::new(param),
                        actual_fold.transform_domain(expr)?,
                    );
                }
                expr if domain_reads_a_row(&expr) => {
                    correlated.push((param, expr));
                }
                expr => staged.push((param, expr)),
            }
        }
        if !correlated.is_empty() {
            {
                // THE ROW CROSSES AS ONE FACT, taken above before the
                // actuals' position borrowed the caller's: the correlated
                // actuals resolve while it stands in frame, and it is left
                // from that frame once they have.
                let (source_relation, leading, absorbs_join_input) = row_in_frame
                    .take()
                    .expect("a correlated actual is one that reads a row, judged above");
                let resolved_values = correlated
                    .iter()
                    .map(|(_param, expr)| {
                        let resolved = actual_fold.transform_domain(expr.clone())?;
                        residual_expression_reads_only(
                            &resolved,
                            source_relation,
                            &actual_fold.core.identities,
                        )?;
                        Ok(resolved)
                    })
                    .collect::<Result<Vec<_>>>()?;
                if carriers_framed {
                    actual_fold.lexical.leave_carriers();
                }
                let mut source = actual_fold.lexical.leave();
                let evaluation_ports = crate::relation::published_ports(
                    &actual_fold.core.identities,
                    &source.semantic_relation(),
                )?;
                let resolved_values = resolved_values
                    .into_iter()
                    .map(|resolved| {
                        anchor_capture_expression(
                            &actual_fold.core.identities,
                            source.semantic_relation(),
                            resolved,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                // A closed value constructed over several standing rows is
                // one value per row. Mint that identity while the exact
                // evaluation relation is owned; no later phase may infer it
                // from a configured value or from the row's spellings.
                let ordering = evaluation_ports
                    .iter()
                    .copied()
                    .map(|column| crate::pipeline::asts::core::OrderingSpec {
                        column: ast_resolved::DomainExpression::Reference(Reference::Named(
                            NamedReference(ColumnOccurrence::engine(column)),
                        )),
                        direction: Some(crate::pipeline::asts::core::OrderDirection::Ascending),
                    })
                    .collect();
                let input = source.semantic_relation();
                let (token_step, _) = actual_fold.core.identities.authority().bind(
                    crate::relation::pending::Pending::WindowWitness {
                        input,
                        partition: Vec::new(),
                        ordering,
                    },
                )?;
                source = source.republished(|chain| {
                    actual_fold
                        .core
                        .identities
                        .authority()
                        .reland(chain, token_step)
                })?;
                let source_ports = crate::relation::published_ports(
                    &actual_fold.core.identities,
                    &source.semantic_relation(),
                )?;
                let token_position = source_ports.len() - 1;
                let mut positions: Vec<_> = source_ports
                    .iter()
                    .copied()
                    .map(|port| crate::relation::pending::Position::Expanded {
                        expr: ast_resolved::DomainExpression::Reference(Reference::Named(
                            NamedReference(ColumnOccurrence::engine(port)),
                        )),
                        naming: None,
                    })
                    .collect();
                positions.extend(resolved_values.into_iter().map(|expr| {
                    crate::relation::pending::Position::Authored { expr, naming: None }
                }));
                let input = source.semantic_relation();
                let (step, _) = actual_fold.core.identities.authority().bind(
                    crate::relation::pending::Pending::Publication {
                        input,
                        publishes: crate::relation::pending::Publishes::Edited,
                        why: crate::relation::form::ProjectWhy::Stage,
                        positions,
                    },
                )?;
                let augmented = source.republished(|chain| {
                    actual_fold.core.identities.authority().reland(chain, step)
                })?;
                let augmented_relation = augmented.semantic_relation();
                let augmented_ports = crate::relation::published_ports(
                    &actual_fold.core.identities,
                    &augmented_relation,
                )?;
                let slots: Vec<_> = augmented_ports
                    .iter()
                    .copied()
                    .enumerate()
                    .map(
                        |(position, source)| crate::relation::form::ProjectSlot::Carried {
                            source,
                            naming: if position < source_ports.len() && absorbs_join_input {
                                crate::relation::form::Naming::Inherited
                            } else {
                                crate::relation::form::Naming::Hygienic
                            },
                        },
                    )
                    .collect();
                let augmented = augmented.republished(|chain| {
                    actual_fold.core.identities.authority().extend(
                        chain,
                        crate::relation::builder::StepOp::Republish {
                            of: crate::relation::builder::Republishing::Project(
                                crate::relation::form::ProjectSpec {
                                    input: augmented_relation,
                                    why: crate::relation::form::ProjectWhy::Restate,
                                    slots: &slots,
                                    dependencies: &[],
                                },
                            ),
                            sources: augmented_ports,
                        },
                    )
                })?;
                actual_fold
                    .core
                    .identities
                    .authority()
                    .mark_row_bounded(&augmented.semantic_relation())?;
                // A zero offset is relational identity and a physical
                // evaluation boundary. The captured expressions belong to
                // this construction row; without the boundary an SQL
                // optimizer may inline the carrier at every later spend.
                let augmented = augmented.transparently(ast_resolved::Transparent::Bound {
                    bound: crate::pipeline::asts::core::TupleOrdinalClause {
                        operator: crate::pipeline::asts::core::TupleOrdinalOperator::GreaterThan,
                        value: 0,
                        offset: None,
                    },
                });
                // THE LANDING IS BOUND INTO THE RECORD as the capture: the
                // record learns the occurrence it stands in place of, the
                // construction token it publishes and whether it absorbs
                // the caller row from this act alone.
                let landing = carriers.bind_capture_landing(
                    augmented,
                    source_relation,
                    absorbs_join_input,
                    &actual_fold.core.identities,
                )?;
                let landing_relation = landing.relation();
                actual_fold
                    .core
                    .identities
                    .authority()
                    .mark_materialized_once(&landing_relation)?;
                let landing_ports = crate::relation::published_ports(
                    &actual_fold.core.identities,
                    &landing_relation,
                )?;
                let landing_token = landing_ports[token_position];
                actual_fold
                    .core
                    .identities
                    .authority()
                    .mark_residual_row_token(landing_token)?;
                let (_values_landing, value_ports, row_token) = if absorbs_join_input {
                    let read = ResolvedRelation::over(
                        CompilerRow::carrier(landing),
                        &actual_fold.core.identities,
                    )?;
                    let read_relation = read.semantic_relation();
                    let read_ports = crate::relation::published_ports(
                        &actual_fold.core.identities,
                        &read_relation,
                    )?;
                    let value_slots: Vec<_> = read_ports
                        .iter()
                        .copied()
                        .map(|source| crate::relation::form::ProjectSlot::Carried {
                            source,
                            naming: crate::relation::form::Naming::Hygienic,
                        })
                        .collect();
                    let scalar = read.republished(|chain| {
                        actual_fold.core.identities.authority().extend(
                            chain,
                            crate::relation::builder::StepOp::Republish {
                                of: crate::relation::builder::Republishing::Project(
                                    crate::relation::form::ProjectSpec {
                                        input: read_relation,
                                        why: crate::relation::form::ProjectWhy::Restate,
                                        slots: &value_slots,
                                        dependencies: &[],
                                    },
                                ),
                                sources: read_ports,
                            },
                        )
                    })?;
                    let value = carriers.bind_capture_value(scalar, &actual_fold.core.identities)?;
                    let value_ports = crate::relation::published_ports(
                        &actual_fold.core.identities,
                        &value.relation(),
                    )?;
                    (
                        value.landing(),
                        value_ports.clone(),
                        value_ports[token_position],
                    )
                } else {
                    (landing.landing(), landing_ports, landing_token)
                };
                let ports = value_ports;
                let row_token = actual_fold
                    .core
                    .identities
                    .authority()
                    .residual_row_token(row_token)
                    .expect("the scalar capture carries its construction row token");
                carriers.capture_token(row_token);
                let captured = ports
                    .get(ports.len().saturating_sub(correlated.len())..)
                    .ok_or_else(|| {
                        DelightQLError::transformation_error(
                            "a residual capture did not publish every configured value",
                            "closed residual construction",
                        )
                    })?;
                for port in captured.iter().copied() {
                    actual_fold
                        .core
                        .identities
                        .authority()
                        .mark_residual_capture_value(port)?;
                }
                for ((param, _), port) in correlated.into_iter().zip(captured.iter().copied()) {
                    values.insert(
                        SqlIdentifier::new(param),
                        crate::pipeline::asts::resolved::DomainExpression::Reference(
                            crate::pipeline::asts::core::Reference::named(
                                crate::pipeline::asts::core::ColumnOccurrence::engine(port),
                            ),
                        ),
                    );
                }
                // The realized row's own leading bindings stand ahead of the
                // landing that reads them.
                carriers.prepend_extra(leading);
                capture = Some(ResidualCapture {
                    row_token,
                    crossing: std::iter::once(ports[token_position])
                        .chain(captured.iter().copied())
                        .collect(),
                });
            }
        }
        for (index, (param, expr)) in staged.into_iter().enumerate() {
            let header = SqlIdentifier::stropped(format!("residual value {index}"));
            let table = crate::pipeline::asts::core::AnonTable::from_values(
                Some(vec![
                    ast_unresolved::DomainExpression::lvar_builder(header).build()
                ]),
                vec![vec![expr]],
            )
            .expect("a residual scalar carrier has a nonempty heading and row");
            let source = ast_unresolved::Chain::authored(ast_unresolved::GroundForm::Literal(
                crate::pipeline::asts::core::AnonRelation::plain(table),
            ));
            let resolved = super::call::resolve_carrier(
                &mut actual_fold,
                &mut carriers,
                crate::relation::form::HoPart::ScalarInput,
                source,
            )?;
            let ports = crate::relation::published_ports(
                &actual_fold.core.identities,
                &resolved.relation(),
            )?;
            if ports.len() != 1 {
                return Err(DelightQLError::transformation_error(
                    "a residual scalar carrier did not publish its configured value",
                    "closed residual construction",
                ));
            }
            // THE STAGED CARRIER COMES INTO VIEW where its read resolves:
            // the record that bound it is what the world answers from.
            actual_fold.env.adopt_carriers(&carriers);
            let read = ast_unresolved::Chain::read(
                ast_unresolved::Relation::Ground {
                    mention: ast_unresolved::GroundMention::Structural {
                        pending: resolved.landing(),
                        authored_name: None,
                        alias: None,
                    },
                    outer: false,
                },
                ast_unresolved::Access::All,
            );
            let scalar = crate::pipeline::asts::core::ScalarizedRelation::authored(
                read,
                crate::pipeline::asts::core::Scalarization::BoundToOne {
                    ordering: Vec::new(),
                },
            );
            let value = ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Scalarized(
                    ast_unresolved::ScalarRelation::Sourceless {
                        body: Box::new(scalar),
                    },
                ),
            );
            values.insert(
                SqlIdentifier::new(param),
                actual_fold.transform_domain(value)?,
            );
        }
    }
    Ok(PreparedResidualPrefix {
        actuals: crate::defuse::bound_use::HoActuals {
            carriers,
            bindings,
            values,
            authored_bare,
            rules,
        },
        capture,
    })
}

fn domain_reads_a_row(expr: &ast_unresolved::DomainExpression) -> bool {
    use crate::pipeline::ast_visit::{walk_visit_domain, AstVisit, Descent};
    struct ReadsRow(bool);
    impl AstVisit<crate::pipeline::asts::core::Unresolved> for ReadsRow {
        fn enter_domain(&mut self, expr: &ast_unresolved::DomainExpression) -> Result<Descent> {
            if matches!(expr, ast_unresolved::DomainExpression::Reference(_)) {
                self.0 = true;
                return Ok(Descent::Break);
            }
            Ok(Descent::Continue)
        }
    }
    let mut reads = ReadsRow(false);
    let _ = walk_visit_domain(&mut reads, expr);
    reads.0
}

fn residual_expression_reads_only(
    expression: &ast_resolved::DomainExpression,
    source: crate::relation::SemanticRelation,
    identities: &crate::relation::Planning,
) -> Result<()> {
    use crate::pipeline::ast_visit::{walk_visit_domain, AstVisit, Descent};

    struct References(Vec<crate::relation::PortId>);
    impl AstVisit<crate::pipeline::asts::core::Resolved> for References {
        fn enter_domain(&mut self, expression: &ast_resolved::DomainExpression) -> Result<Descent> {
            if let ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence { column, .. },
            ))) = expression
            {
                self.0.push(*column);
            }
            Ok(Descent::Continue)
        }
    }

    let mut references = References(Vec::new());
    walk_visit_domain(&mut references, expression)?;
    let authority = identities.authority();
    for reference in references.0 {
        if !authority.carries(&source, reference)? {
            return Err(DelightQLError::validation_error_categorized(
                "resolution/ho/residual-capture",
                "a configured rule-value expression reads outside its construction row",
                "bind the complete configured value in the caller row before constructing the residual",
            ));
        }
    }
    Ok(())
}

/// Move an already-resolved configured expression onto the exact positions
/// published by its construction carrier. Every reference was proved to be
/// carried by that relation immediately before this total translation.
fn anchor_capture_expression(
    identities: &crate::relation::Planning,
    input: crate::relation::SemanticRelation,
    value: ast_resolved::DomainExpression,
) -> Result<ast_resolved::DomainExpression> {
    use crate::pipeline::ast_transform::{walk_transform_domain, AstTransform};
    use crate::pipeline::asts::core::Resolved;

    struct Anchor<'a> {
        identities: &'a crate::relation::Planning,
        input: crate::relation::SemanticRelation,
    }

    impl AstTransform<Resolved, Resolved> for Anchor<'_> {
        crate::pipeline::ast_transform::same_phase_payload_folds!(Resolved);

        fn transform_domain(
            &mut self,
            expression: ast_resolved::DomainExpression,
        ) -> Result<ast_resolved::DomainExpression> {
            if let ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                occurrence,
            ))) = &expression
            {
                let column = self
                    .identities
                    .authority()
                    .port_in(&self.input, occurrence.column)
                    .map_err(|_| {
                        DelightQLError::transformation_error(
                            "a configured value did not land on its proved construction row",
                            "closed residual construction",
                        )
                    })?;
                return Ok(ast_resolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(occurrence.rebound(column)),
                )));
            }
            walk_transform_domain(self, expression)
        }
    }

    Anchor { identities, input }.transform_domain(value)
}

fn is_bare_row_reference(expr: &ast_unresolved::DomainExpression) -> bool {
    matches!(
        expr,
        ast_unresolved::DomainExpression::Reference(Reference::Named(_))
    )
}

/// Close one rule-valued actual while an effect invocation is still standing
/// in its caller world. The effect lifecycle enters the same constructor as
/// a pure consumer; only the surrounding plan walk differs.
/// AN EFFECT'S RESIDUAL: its evaluation row, when it has one, is the plan
/// scratch the effect was staged into, and the residual stands over it
/// for exactly this construction. The carrier its configured values are
/// read from is the row its clauses join.
pub(in crate::defuse) fn construct_effect_residual(
    designator: &ast_unresolved::Chain,
    expected: &crate::pipeline::asts::core::definitions::ResidualSignature,
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold<'_, '_>,
    evaluation_relation: Option<crate::relation::ScratchRow>,
    leading_ctes: Vec<crate::pipeline::asts::resolved::CteBinding>,
) -> Result<RuleValueId> {
    // AN EFFECT INVOCATION HAS NO JOIN TO ITS LEFT. Its evaluation row, when
    // it has one, is already a plan scratch; where it has none, the caller
    // row at this site is simply absent.
    let mut absent = crate::pipeline::resolver::CallerRow::Absent;
    let evaluation_row = match evaluation_relation {
        Some(row) => ResidualEvaluationRow::Realized(ResidualCaptureSource::scratch(row)),
        None => ResidualEvaluationRow::Caller(&mut absent),
    };
    // THE EVALUATION RELATION IS THE ROW THE ACTUALS STAND IN: the
    // resolver's read of its plan mention is entered as a frame for exactly
    // this construction and left again after it.
    if let Some(row) = evaluation_relation {
        let evaluation =
            ResolvedRelation::over(CompilerRow::scratch(row), &fold.core.identities)?;
        fold.lexical
            .enter(evaluation, crate::pipeline::resolver::Reach::Row);
    }
    let id = crate::defuse::ho::construct_residual(designator, expected, fold, evaluation_row);
    if evaluation_relation.is_some() {
        let _evaluation = fold.lexical.leave();
    }
    let id = id?;
    let mut residual = fold.core.residuals.get(id);
    residual.prefix.carriers.append_extra(leading_ctes);
    residual.prefix.carriers.effect_capture_is_join_input();
    Ok(fold.core.residuals.insert(residual))
}
