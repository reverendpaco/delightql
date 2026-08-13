// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The single binder for relation access patterns.

use crate::error::{DelightQLError, Result};
use crate::names::{
    AddressError, Addressing, ColId, Reference, Registry, Republish, ScopeEnv, ScopeOrigin,
};
use crate::pipeline::ast_transform::{walk_transform_boolean, walk_transform_domain, AstTransform};
use crate::pipeline::asts::core::TruthAsValue;
use crate::pipeline::asts::core::{AuthoredColumn, ColumnOccurrence};
use crate::pipeline::asts::core::{Comparison, Existence, RelationalMembership, SigmaApplication};
use crate::pipeline::asts::core::{DomainExpression, Resolved, TruthExpression, Unresolved};
use crate::pipeline::asts::core::{NamedReference, Reference as AstReference};
use crate::pipeline::asts::unresolved::LiteralValue;
use crate::pipeline::asts::{resolved as ast_resolved, unresolved as ast_unresolved};
use crate::pipeline::asts::vocabulary::Vec1;
use delightql_types::SqlIdentifier;

#[derive(Debug, Clone)]
pub enum NormalizedColumnSpec {
    All,
    AllWithUsing(Vec<SqlIdentifier>),
    Explicit(Vec<ColumnSelection>),
}

#[derive(Debug, Clone)]
pub struct ColumnSelection {
    pub source_position: usize,
    pub output_name: String,
    pub constraint: Option<PatternConstraint>,
    /// The slot as the caller wrote it, WHOLE. Kept so the resolved relation
    /// can be rebuilt slot-for-slot: a slot that bound a name is replaced by
    /// the occurrence it bound, and every other slot keeps its authored
    /// form, which names nothing and so carries no spelling past resolution.
    ///
    /// The complete slot, not a domain term read back off it. Storing only
    /// the term erased the crossing here exactly as reading it back erased
    /// it in the visitor: a crossed constraint and an authored `_` became
    /// the same resolved access.
    pub authored: ast_unresolved::Slot,
}

#[derive(Debug, Clone)]
pub enum PatternConstraint {
    Literal(LiteralValue),
    Reference(QualifiedColumnRef),
    Skip,
    SelfUnify {
        first_position: usize,
    },
    Expression(Box<ast_unresolved::DomainExpression>),
    /// The licensed crossing standing in a slot: the column unifies with a
    /// truth read as a VALUE.
    /// The slot's constraint is the CROSSING. The truth itself rides on the
    /// slot, with the column it unifies with; this says only that the
    /// position is constrained rather than bound.
    Crossing,
}

#[derive(Debug, Clone)]
pub struct QualifiedColumnRef {
    #[allow(dead_code)]
    pub table: Option<String>,
    pub column: String,
}

#[derive(Debug)]
pub struct JoinContext {
    pub left_columns: Vec<ColId>,
}

impl JoinContext {
    pub fn from(columns: &[ColId]) -> Self {
        Self {
            left_columns: columns.to_vec(),
        }
    }

    fn find_column(
        &self,
        name: &str,
        qualifier: Option<&str>,
        registry: &Registry,
    ) -> Result<ColId> {
        let name_spelling = registry.intern(name, false);
        let qualifier = qualifier.map(|value| registry.canonical(registry.intern(value, false)));
        let visible = self
            .left_columns
            .iter()
            .copied()
            .map(|column| registry.scope_of(column))
            .collect();
        let environment = ScopeEnv::among(visible, self.left_columns.clone());
        let reference = Reference {
            qualifier,
            name: registry.canonical(name_spelling),
        };
        registry
            .address(reference, &environment)
            .map_err(|error| match error {
                AddressError::NotFound | AddressError::NoSuchScope => {
                    DelightQLError::column_not_found_error(name, "in positional join pattern")
                }
                AddressError::Ambiguous => DelightQLError::validation_error_categorized(
                    "resolution/ambiguous",
                    "Positional join reference matches more than one left-hand column",
                    "qualify the reference with one visible relation",
                ),
                AddressError::Incomplete => super::opaque_reference_refusal(),
                // A positional slot is not a place `_` can be written as a
                // qualifier, so the deictic refusals cannot arise here.
                AddressError::NoUnnamedPipe | AddressError::TwoUnnamedPipes => {
                    DelightQLError::column_not_found_error(name, "in positional join pattern")
                }
            })
    }
}

#[derive(Debug, Clone)]
pub struct BoundHeading(Vec<ColId>);

impl BoundHeading {
    fn identity(source: &[ColId]) -> Self {
        Self(source.to_vec())
    }

    pub fn columns(&self) -> &[ColId] {
        &self.0
    }

    pub fn into_vec(self) -> Vec<ColId> {
        self.0
    }
}

#[derive(Debug)]
pub struct PatternResult {
    pub output_scope: crate::names::ScopeId,
    pub output_columns: BoundHeading,
    pub where_constraints: Vec<ast_resolved::TruthExpression>,
    pub using_columns: Option<Vec<SqlIdentifier>>,
    /// Slot-aligned occurrences for an explicit positional pattern; `None`
    /// for the glob shapes, which have no slots. Unlike `output_columns`,
    /// this keeps the skipped slots, so index i is still the caller's slot i.
    occurrences: Option<Vec<ast_resolved::Slot>>,
}

fn contains_identifier(names: &[SqlIdentifier], candidate: &str) -> bool {
    names
        .iter()
        .any(|name| SqlIdentifier::str_eq(name.as_str(), candidate))
}

pub(super) fn apply_local_constraints(
    source: ast_resolved::Chain,
    constraints: Vec<ast_resolved::TruthExpression>,
    output_scope: crate::names::ScopeId,
) -> ast_resolved::Chain {
    let Some(condition) = ast_resolved::TruthExpression::all(constraints) else {
        return source;
    };

    source.then(ast_resolved::Continuation::Restrict {
        condition: condition,
        origin: ast_resolved::FilterOrigin::PositionalLiteral {
            source: output_scope,
        },
        cpr_schema: output_scope,
    })
}

impl PatternResult {
    /// The access the resolved relation carries.
    ///
    /// Resolution is where authored characters stop. A relation that has been
    /// bound already answers "which columns" through its output scope, so the
    /// spec beside it is a second statement of the same thing — and if it is
    /// the caller's text, the two can disagree and downstream readers must
    /// re-resolve names the binder already resolved. Each slot that bound a
    /// name therefore carries the occurrence it bound; the glob shapes carry
    /// nothing to resolve and pass through.
    pub(super) fn resolved_spec(
        &self,
        authored: &ast_unresolved::Access,
    ) -> Result<ast_resolved::Access> {
        match (&self.occurrences, authored) {
            (Some(occurrences), ast_unresolved::Access::Slots(_)) => {
                Vec1::try_from_vec(occurrences.clone())
                    .map(ast_resolved::Access::Slots)
                    .ok_or_else(|| {
                        DelightQLError::validation_error(
                            "A positional pattern bound no slots",
                            "Pattern resolution",
                        )
                    })
            }
            (Some(_), _) => Err(DelightQLError::validation_error(
                "A non-positional pattern produced bound slot occurrences",
                "Pattern resolution",
            )),
            (None, ast_unresolved::Access::All) => Ok(ast_resolved::Access::All),
            (None, ast_unresolved::Access::Unasked) => Ok(ast_resolved::Access::Unasked),
            (None, ast_unresolved::Access::Dequalify(columns)) => {
                Ok(ast_resolved::Access::Dequalify(columns.clone()))
            }
            (None, ast_unresolved::Access::DequalifyAll) => Ok(ast_resolved::Access::DequalifyAll),
            (None, ast_unresolved::Access::Slots(_)) => Err(DelightQLError::validation_error(
                "A positional pattern reached resolution without bound occurrences",
                "Positional pattern resolution",
            )),
        }
    }
}

pub struct PatternResolver<'a> {
    /// The innermost open instantiation's formals, when a consulted value
    /// definition's body is what authored this pattern.
    formal_frame: Option<&'a super::FormalFrame>,
    /// A slot expression may invoke a definition; the sources travel WITH
    /// the compilation's allowance, or the pattern cannot instantiate at
    /// all. "Can instantiate but unbounded" is unrepresentable.
    instantiation: Option<super::SlotInstantiation<'a>>,
}

impl Default for PatternResolver<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> PatternResolver<'a> {
    /// The narrower operation: conversion that CANNOT instantiate a
    /// definition. Not a permissive default — a pattern road that can meet
    /// a definition call constructs `with_formals` and owns the allowance.
    pub fn new() -> Self {
        Self {
            formal_frame: None,
            instantiation: None,
        }
    }

    pub(crate) fn with_formals(
        formal_frame: Option<&'a super::FormalFrame>,
        instantiation: Option<super::SlotInstantiation<'a>>,
    ) -> Self {
        Self {
            formal_frame,
            instantiation,
        }
    }

    pub fn resolve_pattern(
        &self,
        pattern: &ast_unresolved::Access,
        table_schema: &[ColId],
        table_name: &str,
        join_context: Option<&JoinContext>,
        registry: &Registry,
    ) -> Result<PatternResult> {
        let normalized = self.normalize_pattern(pattern, table_schema)?;
        self.resolve_normalized(normalized, table_schema, table_name, join_context, registry)
    }

    fn normalize_pattern(
        &self,
        pattern: &ast_unresolved::Access,
        table_schema: &[ColId],
    ) -> Result<NormalizedColumnSpec> {
        match pattern {
            ast_unresolved::Access::All
            | ast_unresolved::Access::Unasked
            | ast_unresolved::Access::DequalifyAll => Ok(NormalizedColumnSpec::All),
            ast_unresolved::Access::Dequalify(columns) => {
                Ok(NormalizedColumnSpec::AllWithUsing(columns.clone()))
            }
            ast_unresolved::Access::Slots(slots) => Ok(NormalizedColumnSpec::Explicit(
                self.positional_to_selections(slots, table_schema)?,
            )),
        }
    }

    pub(super) fn positional_to_selections(
        &self,
        slots: &Vec1<ast_unresolved::Slot>,
        table_schema: &[ColId],
    ) -> Result<Vec<ColumnSelection>> {
        let mut selections = Vec::new();
        let mut first_slot_of: Vec<(String, usize)> = Vec::new();
        for (position, slot) in slots.iter().enumerate() {
            if position >= table_schema.len() {
                return Err(DelightQLError::parse_error(format!(
                    "Positional pattern has {} elements but table has only {} columns",
                    slots.len(),
                    table_schema.len(),
                )));
            }
            // A non-empty output_name marks the slots that bind: the binder
            // slots and the qualified references. Every other slot names
            // nothing, and the two are told apart by this emptiness downstream.
            let (output_name, constraint) = match slot {
                ast_unresolved::Slot::Bind(binder) => {
                    let name = &binder.name;
                    if let Some(&(_, first_position)) = first_slot_of.iter().find(|(seen, _)| {
                        delightql_types::SqlIdentifier::str_eq(seen, name.as_str())
                    }) {
                        (
                            name.to_string(),
                            Some(PatternConstraint::SelfUnify { first_position }),
                        )
                    } else {
                        first_slot_of.push((name.to_string(), position));
                        (name.to_string(), None)
                    }
                }
                ast_unresolved::Slot::Anon => (String::new(), Some(PatternConstraint::Skip)),
                ast_unresolved::Slot::Reuse(NamedReference(AuthoredColumn {
                    name,
                    qualifier,
                    ..
                })) => (
                    name.to_string(),
                    Some(PatternConstraint::Reference(QualifiedColumnRef {
                        table: qualifier.as_ref().map(ToString::to_string),
                        column: name.to_string(),
                    })),
                ),
                ast_unresolved::Slot::Constraint(constraint) => match constraint {
                    ast_unresolved::SlotConstraint::Value(term) => match &**term {
                        ast_unresolved::DomainExpression::Application(
                            ast_unresolved::FunctionApplication::Ground(value),
                        ) => (
                            String::new(),
                            Some(PatternConstraint::Literal(value.clone())),
                        ),
                        other => (
                            String::new(),
                            Some(PatternConstraint::Expression(Box::new(other.clone()))),
                        ),
                    },
                    // The crossing constrains the column with a truth read as
                    // a VALUE; it is not a predicate over the row. The truth
                    // itself travels on the SLOT — this only says the
                    // position is constrained.
                    ast_unresolved::SlotConstraint::Truth { .. } => {
                        (String::new(), Some(PatternConstraint::Crossing))
                    }
                },
            };
            selections.push(ColumnSelection {
                source_position: position,
                output_name,
                constraint,
                authored: slot.clone(),
            });
        }
        Ok(selections)
    }

    fn resolve_normalized(
        &self,
        spec: NormalizedColumnSpec,
        table_schema: &[ColId],
        table_name: &str,
        join_context: Option<&JoinContext>,
        registry: &Registry,
    ) -> Result<PatternResult> {
        match spec {
            NormalizedColumnSpec::All => {
                let output_scope = registry.common_scope(table_schema).ok_or_else(|| {
                    DelightQLError::parse_error("A relation pattern requires one source heading")
                })?;
                Ok(PatternResult {
                    output_scope,
                    output_columns: BoundHeading::identity(table_schema),
                    where_constraints: Vec::new(),
                    using_columns: None,
                    occurrences: None,
                })
            }
            NormalizedColumnSpec::AllWithUsing(columns) => {
                let output_scope = registry.common_scope(table_schema).ok_or_else(|| {
                    DelightQLError::parse_error("A relation pattern requires one source heading")
                })?;
                // The correspondence is settled by whoever consumes the
                // USING columns — the join road's `Correspond` carrier and
                // the existence road's synthesis, each against the heading
                // of the relation the step continues. Deriving conditions
                // here read the whole outer context instead, so a sibling
                // existence scope could make an unambiguous name refuse as
                // ambiguous — and every consumer discarded the conditions.
                Ok(PatternResult {
                    output_scope,
                    output_columns: BoundHeading::identity(table_schema),
                    where_constraints: Vec::new(),
                    using_columns: Some(columns),
                    occurrences: None,
                })
            }
            NormalizedColumnSpec::Explicit(selections) => self.resolve_explicit_selections(
                selections,
                table_schema,
                table_name,
                join_context,
                registry,
            ),
        }
    }

    fn resolve_explicit_selections(
        &self,
        selections: Vec<ColumnSelection>,
        table_schema: &[ColId],
        table_name: &str,
        join_context: Option<&JoinContext>,
        registry: &Registry,
    ) -> Result<PatternResult> {
        let input = registry.common_scope(table_schema).ok_or_else(|| {
            DelightQLError::parse_error("A positional pattern requires one source heading")
        })?;
        let spelling = registry.intern(table_name, false);
        let output_scope = registry.mint_derived_scope(
            ScopeOrigin::UserAlias { of: input },
            crate::names::Hint::User(spelling),
        );
        let mut output = Vec::new();
        let mut where_constraints = Vec::new();
        let mut using_columns = Vec::new();
        let mut occurrences = Vec::with_capacity(selections.len());

        for selection in selections {
            let source = table_schema[selection.source_position];
            if matches!(selection.constraint, Some(PatternConstraint::Skip)) {
                occurrences.push(resolve_authored_slot(
                    &selection.authored,
                    source,
                    table_schema,
                    registry,
                    self.formal_frame,
                    self.instantiation,
                )?);
                continue;
            }
            let output_spelling = if selection.output_name.is_empty() {
                registry.published(source)
            } else {
                Some(registry.intern(&selection.output_name, false))
            };
            let constrained = matches!(
                selection.constraint,
                Some(
                    PatternConstraint::Literal(_)
                        | PatternConstraint::SelfUnify { .. }
                        | PatternConstraint::Expression(_)
                        | PatternConstraint::Crossing
                )
            );
            let column = registry.republish_column(
                source,
                output_scope,
                if selection.output_name.is_empty()
                    || registry.published_sym(source)
                        == output_spelling.map(|spelling| registry.canonical(spelling))
                {
                    Republish::Passthrough
                } else {
                    Republish::Rename
                },
                output_spelling,
                if constrained {
                    Addressing::Hygienic
                } else {
                    Addressing::Bare
                },
                |_| {},
            );

            let qualified_reference = matches!(selection.authored, ast_unresolved::Slot::Reuse(_));
            occurrences.push(if selection.output_name.is_empty() {
                // Names nothing, so there is no occurrence to point at: a
                // literal or a computed slot constrains the source column but
                // does not bind it to a caller-written name. Its interior is
                // still resolved against this relation's own heading — the
                // same heading the constraint beside it is resolved against.
                resolve_authored_slot(
                    &selection.authored,
                    source,
                    table_schema,
                    registry,
                    self.formal_frame,
                    self.instantiation,
                )?
            } else if qualified_reference {
                // A qualified reference names somebody else's column: it
                // constrains this position, it does not offer a name for it.
                ast_resolved::Slot::Reuse(NamedReference(ColumnOccurrence {
                    column,
                    explicit_qualifier: true,
                }))
            } else {
                // A bare name bound. The slot stays a binding slot; the phase
                // changed what it holds, not what it is.
                ast_resolved::Slot::Bind(column)
            });

            match selection.constraint {
                Some(PatternConstraint::Literal(value)) => {
                    where_constraints.push(create_literal_constraint(source, value));
                }
                // The crossing constrains the column with a truth read as a
                // VALUE — never a row predicate. The pair that says so is on
                // the resolved SLOT, and the null-safe unification is spelled
                // at lowering, where the column is already in scope.
                Some(PatternConstraint::Crossing) => {}
                Some(PatternConstraint::Reference(reference)) => {
                    let context = join_context.ok_or_else(|| {
                        DelightQLError::column_not_found_error(
                            reference.column.clone(),
                            "a qualified positional binding requires a left-hand relation",
                        )
                    })?;
                    // The left column is ADDRESSED here for its refusals — a
                    // name no visible relation publishes, or one two of them
                    // do — and the correspondence itself is settled by the
                    // consumer, from the USING name below.
                    context.find_column(&reference.column, reference.table.as_deref(), registry)?;
                    // A caller-pattern binder's name is characters in this
                    // carrier; reading it unstropped is what it has meant.
                    using_columns.push(SqlIdentifier::new(selection.output_name.clone()));
                }
                Some(PatternConstraint::SelfUnify { first_position }) => {
                    where_constraints.push(create_self_unification_condition(
                        table_schema[first_position],
                        source,
                    ));
                }
                Some(PatternConstraint::Expression(expression)) => {
                    where_constraints.push(create_expression_constraint(
                        source,
                        &expression,
                        table_schema,
                        registry,
                        self.formal_frame,
                        self.instantiation,
                    )?);
                }
                Some(PatternConstraint::Skip) => unreachable!("skip handled before binding"),
                None => {}
            }
            output.push(column);

            if let Some(context) = join_context {
                if !contains_identifier(&using_columns, &selection.output_name)
                    && !selection.output_name.is_empty()
                {
                    let spelling = registry.intern(&selection.output_name, false);
                    let name = registry.canonical(spelling);
                    let declared: Vec<_> = context
                        .left_columns
                        .iter()
                        .copied()
                        .filter(|column| {
                            matches!(
                                registry.addressing(*column),
                                Addressing::Bare | Addressing::BareAnswering(_)
                            ) && registry.published_sym(*column) == Some(name)
                        })
                        .collect();
                    // A positional lvar unifies with a BARE lvar of the same
                    // name and with nothing else. Qualification is part of an
                    // lvar's complete name: a glob writes
                    // `lt.k`, and a positional `k` is a different name, so
                    // they neither unify nor collide — the relations cross.
                    // Reading the shared final segment as a collision refused
                    // a legal query, and asymmetrically: the same fact
                    // written the other way round already crossed.
                    // EXACTLY ONE declares the name, or the positional lvar
                    // corresponds with nothing here.
                    if let [_one] = declared.as_slice() {
                        // A caller-pattern binder's name is characters in this
                        // carrier; reading it unstropped is what it has meant.
                        using_columns.push(SqlIdentifier::new(selection.output_name.clone()));
                    }
                }
            }
        }

        Ok(PatternResult {
            output_scope,
            output_columns: BoundHeading(output),
            where_constraints,
            using_columns: (!using_columns.is_empty()).then_some(using_columns),
            occurrences: Some(occurrences),
        })
    }

}

fn resolved_ref(column: ColId) -> ast_resolved::DomainExpression {
    ast_resolved::DomainExpression::Reference(AstReference::Named(NamedReference(
        ColumnOccurrence {
            column,
            explicit_qualifier: false,
        },
    )))
}

fn create_literal_constraint(column: ColId, value: LiteralValue) -> ast_resolved::TruthExpression {
    ast_resolved::TruthExpression::Comparison(Comparison {
        operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
        left: Box::new(resolved_ref(column)),
        right: Box::new(ast_resolved::DomainExpression::Application(
            ast_resolved::FunctionApplication::Ground(value),
        )),
    })
}

/// THE AUTHORED SLOT, RESOLVED — slot-for-slot, kind for kind.
///
/// A crossed slot stays a crossed constraint, and it takes the COLUMN it
/// unifies with with it: the unification is null-safe and it is spelled at
/// lowering, from this pair. Reading the slot back as a domain term and
/// classifying that made a crossing and an authored `_` the same resolved
/// access, which is the erasure this road exists to prevent.
fn resolve_authored_slot(
    slot: &ast_unresolved::Slot,
    constrained: ColId,
    table_schema: &[ColId],
    registry: &Registry,
    formal_frame: Option<&super::FormalFrame>,
    instantiation: Option<super::SlotInstantiation<'_>>,
) -> Result<ast_resolved::Slot> {
    Ok(match slot {
        ast_unresolved::Slot::Constraint(ast_unresolved::SlotConstraint::Truth {
            value, ..
        }) => {
            let mut converter = StrictPhaseConverter {
                heading: table_schema,
                registry,
                formal_frame,
                instantiation,
            };
            ast_resolved::Slot::Constraint(ast_resolved::SlotConstraint::Truth {
                column: constrained,
                value: TruthAsValue(converter.transform_boolean(value.truth().clone())?),
            })
        }
        // The anonymous slot is STRUCTURE, not a value: it crosses as
        // itself, never through the value road — its authored term is the
        // open leaf, which resolution spends only at an applying position.
        ast_unresolved::Slot::Anon => ast_resolved::Slot::Anon,
        other => {
            let term = other
                .term()
                .expect("only a crossed slot or the anonymous slot has no term road, and both are handled above");
            ast_resolved::Slot::classify(convert_unresolved_to_resolved_expression(
                &term,
                table_schema,
                registry,
                formal_frame,
                instantiation,
            )?)
        }
    })
}

fn create_self_unification_condition(left: ColId, right: ColId) -> ast_resolved::TruthExpression {
    ast_resolved::TruthExpression::Comparison(Comparison {
        operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
        left: Box::new(resolved_ref(left)),
        right: Box::new(resolved_ref(right)),
    })
}

fn create_expression_constraint(
    column: ColId,
    expression: &ast_unresolved::DomainExpression,
    table_schema: &[ColId],
    registry: &Registry,
    formal_frame: Option<&super::FormalFrame>,
    instantiation: Option<super::SlotInstantiation<'_>>,
) -> Result<ast_resolved::TruthExpression> {
    Ok(ast_resolved::TruthExpression::Comparison(Comparison {
        operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
        left: Box::new(resolved_ref(column)),
        right: Box::new(convert_unresolved_to_resolved_expression(
            expression,
            table_schema,
            registry,
            formal_frame,
            instantiation,
        )?),
    }))
}

struct StrictPhaseConverter<'a> {
    heading: &'a [ColId],
    registry: &'a Registry,
    formal_frame: Option<&'a super::FormalFrame>,
    /// Definition sources + the compilation's ONE allowance, together or
    /// not at all.
    instantiation: Option<super::SlotInstantiation<'a>>,
}

impl AstTransform<Unresolved, Resolved> for StrictPhaseConverter<'_> {
    crate::pipeline::ast_transform::position_is_resolved_against_a_heading!();
    fn fold_entity(
        &mut self,
        entity: crate::pipeline::asts::vocabulary::Ref,
    ) -> crate::error::Result<crate::names::CallableId> {
        Ok(entity.written_call_identity(self.registry))
    }
    crate::pipeline::ast_transform::column_is_bound_where_it_is_resolved!();
    crate::pipeline::ast_transform::binder_is_bound_where_the_pattern_is_resolved!();
    crate::pipeline::ast_transform::a_landing_is_consumed_where_the_pipe_is_applied!();
    crate::pipeline::ast_transform::a_context_marker_is_consumed_where_the_call_instantiates!();
    crate::pipeline::ast_transform::scope_is_minted_where_it_is_resolved!();
    crate::pipeline::ast_transform::minted_where_it_is_decided!(
        fold_recursion -> crate::pipeline::asts::vocabulary::RecursionState: "a binding's recursion",
    );
    fn fold_cte_subject(
        &mut self,
        _: crate::pipeline::asts::core::CteSubject,
    ) -> crate::error::Result<crate::names::ScopeId> {
        Err(crate::error::DelightQLError::transformation_error(
            "a binding's subject is spent where the resolver's CTE road mints its scope, \
             and this fold is not that place",
            "phase_payload",
        ))
    }
    fn fold_cte_authority(
        &mut self,
        _: crate::pipeline::asts::core::CteAuthority,
    ) -> crate::error::Result<()> {
        Err(crate::error::DelightQLError::transformation_error(
            "a binding's head and provenance are spent where the resolver's CTE road \
             mints its scope, and this fold is not that place",
            "phase_payload",
        ))
    }
    crate::pipeline::ast_transform::minted_where_it_is_decided!(
        fold_output -> Option<crate::names::ColId>: "an expression's output occurrence",
        fold_scalar_output -> crate::names::ColId: "a scalarized relation's column",
        fold_destructure -> Vec<crate::pipeline::asts::core::DestructureMapping>: "a destructuring pattern's columns",
    );
    fn fold_open_leaf(
        &mut self,
        _: crate::pipeline::asts::core::DomainHole,
    ) -> crate::error::Result<crate::pipeline::asts::vocabulary::Never> {
        Err(crate::error::DelightQLError::validation_error_categorized(
            "value/open/unapplied",
            "a composition input stands outside any callable applying it",
            "the position that applies an open body spends its slot",
        ))
    }

    fn fold_cover_callable(
        &mut self,
        _: crate::pipeline::asts::core::Callable<crate::pipeline::asts::core::Unresolved>,
    ) -> crate::error::Result<()> {
        Err(crate::error::DelightQLError::transformation_error(
            "a cover's callable is applied where its operator resolves, and this fold is not that place",
            "phase_payload",
        ))
    }

    fn fold_rename_target(
        &mut self,
        _: crate::pipeline::asts::core::NameTarget,
    ) -> crate::error::Result<crate::names::Spelling> {
        Err(crate::error::DelightQLError::transformation_error(
            "a rename target is expanded where the rename resolves, and this fold is not that place",
            "phase_payload",
        ))
    }
    fn fold_drill(
        &mut self,
        _: crate::pipeline::asts::core::operators::AuthoredDrill,
    ) -> crate::error::Result<crate::pipeline::asts::core::operators::BoundDrill> {
        Err(crate::error::DelightQLError::transformation_error(
            "an interior drill binds where its operator resolves, and this fold is not that place",
            "phase_payload",
        ))
    }

    #[stacksafe::stacksafe]
    fn transform_domain(
        &mut self,
        expression: DomainExpression<Unresolved>,
    ) -> Result<DomainExpression<Resolved>> {
        // A slot expression may invoke a query-scoped value definition, and
        // it is spent HERE like at any other site: arguments resolve against
        // this relation's heading, the body against the formals alone.
        if let DomainExpression::Application(
            crate::pipeline::asts::core::FunctionApplication::Standard(application),
        ) = &expression
        {
            if let Some(inlined) = self.instantiate_scoped(application)? {
                return Ok(inlined);
            }
        }
        match expression {
            DomainExpression::Reference(AstReference::Ordinal(_)) => Err(
                DelightQLError::parse_error("Column ordinals not supported in pattern constraints"),
            ),
            // A computed slot's interior sees exactly its own relation's
            // heading: the constraint compares this relation's column to an
            // expression over this relation's row. A bare name binds there;
            // anything the heading does not publish is refused here, before
            // the authored text can travel on unresolved.
            DomainExpression::Reference(AstReference::Named(NamedReference(AuthoredColumn {
                name,
                qualifier: None,
                namespace_path,
            }))) => {
                // A formal of the open instantiation is already spent: it
                // stands for the caller's resolved argument and never binds
                // to this relation's heading, so the definition's own probes
                // cannot capture it.
                if namespace_path.is_empty() {
                    if let Some(resolved) =
                        self.formal_frame.and_then(|frame| frame.values.get(&name))
                    {
                        return Ok(resolved.clone());
                    }
                }
                let sym = self
                    .registry
                    .canonical(self.registry.intern(&name, name.is_stropped()));
                let visible = self
                    .heading
                    .iter()
                    .copied()
                    .map(|column| self.registry.scope_of(column))
                    .collect();
                let environment = ScopeEnv::among(visible, self.heading.to_vec());
                match self.registry.address(
                    Reference {
                        qualifier: None,
                        name: sym,
                    },
                    &environment,
                ) {
                    Ok(column) => Ok(DomainExpression::Reference(AstReference::Named(NamedReference(ColumnOccurrence {
                        column,
                        explicit_qualifier: false,
                    })))),
                    Err(AddressError::Incomplete) => Err(super::opaque_reference_refusal()),
                    Err(
                        AddressError::NotFound
                        | AddressError::NoSuchScope
                        // A bare reference carries no `_` qualifier, so the
                        // deictic refusals have no road into this lookup.
                        | AddressError::NoUnnamedPipe
                        | AddressError::TwoUnnamedPipes,
                    ) => Err(DelightQLError::column_not_found_error(
                        name.to_string(),
                        "in a computed pattern slot",
                    )),
                    Err(AddressError::Ambiguous) => Err(DelightQLError::validation_error(
                        format!(
                            "column '{}' is published more than once in this \
                             relation's heading",
                            name
                        ),
                        "in a computed pattern slot",
                    )),
                }
            }
            DomainExpression::Reference(AstReference::Named(NamedReference(AuthoredColumn {
                ..
            }))) => Err(DelightQLError::parse_error(
                "a qualified reference inside a computed pattern slot cannot bind — \
                 the slot sees only its own relation's heading",
            )),
            other => walk_transform_domain(self, other),
        }
    }

    fn transform_boolean(
        &mut self,
        expression: TruthExpression<Unresolved>,
    ) -> Result<TruthExpression<Resolved>> {
        match expression {
            TruthExpression::Existence(Existence { .. }) => Err(DelightQLError::parse_error(
                "EXISTS expressions not supported in pattern constraints",
            )),
            TruthExpression::RelationalMembership(RelationalMembership { .. }) => {
                Err(DelightQLError::parse_error(
                    "IN subquery expressions not supported in pattern constraints",
                ))
            }
            TruthExpression::Sigma(SigmaApplication { .. }) => {
                Err(DelightQLError::not_implemented(
                    "Sigma predicates in pattern destructuring not yet supported",
                ))
            }
            other => walk_transform_boolean(self, other),
        }
    }
}

impl StrictPhaseConverter<'_> {
    /// A slot expression's invocation of a query-scoped value definition.
    ///
    /// The definition is spent with the same laws as anywhere: arguments
    /// resolve where the caller stands (this relation's heading, plus any
    /// open frame), the body resolves SEALED — its formals and nothing
    /// else. A crossing body resolves into the licensed ClauseSelection
    /// carrier.
    fn instantiate_scoped(
        &mut self,
        application: &ast_unresolved::StandardApplication,
    ) -> Result<Option<ast_resolved::DomainExpression>> {
        use crate::pipeline::asts::core::operators::ScalarArgument;
        let Some(instantiation) = self.instantiation else {
            // The narrower conversion: no definition source travels here,
            // so no call can instantiate — and none can escape the bound.
            return Ok(None);
        };
        let callee = &application.call().callee;
        let name = callee.name_text();
        // A query-scoped name is bare; a consulted one may be qualified.
        let scoped = if callee.namespace_fq().is_none() {
            let key = callee.name_identifier();
            instantiation.scoped_cfes.get(&key).cloned()
        } else {
            None
        };
        let cfe = match scoped {
            Some(cfe) => cfe,
            None => {
                let callee_ident = callee.name_identifier();
                let entity = match crate::pipeline::resolver::grounding::lookup_borrowed_function(
                    &callee_ident,
                    callee.namespace_fq().as_deref(),
                    instantiation.consult,
                    instantiation.lookup_scope,
                )? {
                    Some(entity) => entity,
                    None => {
                        match crate::pipeline::resolver::grounding::lookup_borrowed_context_aware_function(
                            &callee_ident,
                            callee.namespace_fq().as_deref(),
                            instantiation.consult,
                            instantiation.lookup_scope,
                        )? {
                            Some(entity) => entity,
                            None => return Ok(None),
                        }
                    }
                };
                let mut cfe =
                    crate::pipeline::resolver::grounding::consulted_entity_to_cfe_definition(
                        &entity,
                    )?;
                if let Some(ns) = crate::pipeline::resolver::grounding::pre_grounded_data_ns_path(
                    instantiation.consult,
                    &entity.namespace,
                ) {
                    cfe.body =
                        crate::pipeline::resolver::grounding::patch_data_ns_in_body(cfe.body, &ns);
                }
                cfe
            }
        };
        let cfe = &cfe;
        if !cfe.callable_formals().is_empty()
            || cfe.context_mode != crate::pipeline::asts::core::ContextMode::None
        {
            // A curried or context-aware definition has no slot reading; the
            // ordinary refusal for an unknown function stands.
            return Ok(None);
        }
        // The allowance is NOT optional here: the bundle that made this
        // instantiation reachable carries it.
        let _instantiation_frame = instantiation.depth.enter(&name)?;
        let mut values = Vec::new();
        for member in application.call().arguments.scalar_members() {
            match member {
                ScalarArgument::Value(value) => match value.domain() {
                    Some(domain) => values.push(self.transform_domain(domain.clone())?),
                    None => {
                        return Err(DelightQLError::validation_error_categorized(
                            "cfe/crossed_argument",
                            format!(
                                "'{name}' takes values; a truth read as a value cannot stand \
                                 where the definition's formal does"
                            ),
                            "bind the truth to a column first and pass the column",
                        ))
                    }
                },
                _ => return Ok(None),
            }
        }
        let scalar_formals = cfe.scalar_formals();
        if values.len() != scalar_formals.len() {
            return Err(DelightQLError::validation_error_categorized(
                "cfe/arity",
                format!(
                    "'{name}' expects {} argument{}, got {}",
                    scalar_formals.len(),
                    if scalar_formals.len() == 1 { "" } else { "s" },
                    values.len()
                ),
                "supply one value per declared parameter",
            ));
        }
        let mut frame = super::FormalFrame::default();
        for (formal, value) in scalar_formals.iter().zip(values) {
            frame.values.insert(formal.name.clone(), value);
        }
        let mut body_converter = StrictPhaseConverter {
            // Sealed: the body sees its formals, never this heading.
            heading: &[],
            registry: self.registry,
            formal_frame: Some(&frame),
            instantiation: Some(super::SlotInstantiation {
                // The body's own sibling lookups answer under ITS namespace.
                lookup_scope: cfe
                    .source_namespace
                    .as_deref()
                    .or(instantiation.lookup_scope),
                ..instantiation
            }),
        };
        match &cfe.body {
            crate::pipeline::asts::core::OutValue::Domain(body) => {
                body_converter.transform_domain(body.clone()).map(Some)
            }
            crate::pipeline::asts::core::OutValue::Truth(crossing) => body_converter
                .transform_boolean(crossing.clone().into_truth())
                .map(|resolved| {
                    Some(ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::ClauseSelection(
                            crate::pipeline::asts::core::ClauseSelection {
                                arms: vec![crate::pipeline::asts::core::ClauseArm {
                                    guard: None,
                                    result: ast_resolved::OutValue::Truth(TruthAsValue(resolved)),
                                }],
                            },
                        ),
                    ))
                }),
        }
    }
}

fn convert_unresolved_to_resolved_expression(
    expression: &ast_unresolved::DomainExpression,
    table_schema: &[ColId],
    registry: &Registry,
    formal_frame: Option<&super::FormalFrame>,
    instantiation: Option<super::SlotInstantiation<'_>>,
) -> Result<ast_resolved::DomainExpression> {
    StrictPhaseConverter {
        heading: table_schema,
        registry,
        formal_frame,
        instantiation,
    }
    .transform_domain(expression.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::names::{ColumnOrigin, Hint, ValueFacts};

    fn named_column(registry: &Registry, relation: &str, column: &str) -> ColId {
        let relation = registry.intern(relation, false);
        let scope = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::User(relation), None);
        let column = registry.intern(column, false);
        registry.mint_column(
            scope,
            ColumnOrigin::Bound { position: 0 },
            Some(column),
            Addressing::Published,
            ValueFacts::default(),
        )
    }

    #[test]
    fn a_positional_reference_requires_one_left_column() {
        let registry = Registry::new(&[]);
        let left_a = named_column(&registry, "a", "id");
        let left_b = named_column(&registry, "b", "id");
        let context = JoinContext::from(&[left_a, left_b]);

        assert!(context.find_column("missing", None, &registry).is_err());
        assert!(context.find_column("id", None, &registry).is_err());
        assert_eq!(
            context.find_column("id", Some("a"), &registry).unwrap(),
            left_a
        );
    }

    #[test]
    fn using_membership_is_case_insensitive() {
        assert!(contains_identifier(&[SqlIdentifier::new("x")], "X"));
        assert!(contains_identifier(
            &[SqlIdentifier::new("Mixed_Name")],
            "mixed_name"
        ));
        assert!(!contains_identifier(&[SqlIdentifier::new("x")], "y"));
    }
}
