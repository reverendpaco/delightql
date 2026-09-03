// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// ast_resolved.rs - Semantically resolved AST for DelightQL
//
// This module defines the resolved (semantic) AST that comes from
// the resolver phase. It contains the same structure as ast_unresolved
// but with added semantic information on relation-producing nodes.
//
// Key additions over ast_unresolved:
// 1. a scope on every relation-producing node, naming what it publishes
// 2. resolved referents in place of written names

// Type aliases for resolved phase
pub type Query = crate::pipeline::asts::core::Query<crate::pipeline::asts::core::Resolved>;
pub type CteBinding =
    crate::pipeline::asts::core::CteBinding<crate::pipeline::asts::core::Resolved>;
pub type Chain = crate::pipeline::asts::core::Chain<crate::pipeline::asts::core::Resolved>;
pub type GroundForm =
    crate::pipeline::asts::core::GroundForm<crate::pipeline::asts::core::Resolved>;
pub type Step = crate::pipeline::asts::core::Step<crate::pipeline::asts::core::Resolved>;
pub type Grelex = crate::pipeline::asts::core::Grelex<crate::pipeline::asts::core::Resolved>;
#[allow(dead_code)]
pub type Peel = crate::pipeline::asts::core::Peel<crate::pipeline::asts::core::Resolved>;
#[allow(dead_code)]
#[allow(dead_code)]
pub type Transparent =
    crate::pipeline::asts::core::Transparent<crate::pipeline::asts::core::Resolved>;
pub type Continuation =
    crate::pipeline::asts::core::Continuation<crate::pipeline::asts::core::Resolved>;
pub type StructuralStep =
    crate::pipeline::asts::core::StructuralStep<crate::pipeline::asts::core::Resolved>;
pub type StructuralForm =
    crate::pipeline::asts::core::StructuralForm<crate::pipeline::asts::core::Resolved>;
pub type AnonTable = crate::pipeline::asts::core::AnonTable<crate::pipeline::asts::core::Resolved>;
pub type AnonRelation =
    crate::pipeline::asts::core::AnonRelation<crate::pipeline::asts::core::Resolved>;
pub type Datum = crate::pipeline::asts::core::Datum<crate::pipeline::asts::core::Resolved>;
pub type HeaderItem =
    crate::pipeline::asts::core::HeaderItem<crate::pipeline::asts::core::Resolved>;
pub type TabularRow<T> = crate::pipeline::asts::core::TabularRow<T>;
pub type TabularBody<H, D> = crate::pipeline::asts::core::TabularBody<H, D>;
pub type WholeHeading =
    crate::pipeline::asts::core::WholeHeading<crate::pipeline::asts::core::Resolved>;
pub type CorrPred = crate::pipeline::asts::core::CorrPred<crate::pipeline::asts::core::Resolved>;
pub type MemberCorrelation =
    crate::pipeline::asts::core::MemberCorrelation<crate::pipeline::asts::core::Resolved>;
pub use crate::pipeline::asts::core::Correspondence;
pub type BagCorrelation =
    crate::pipeline::asts::core::BagCorrelation<crate::pipeline::asts::core::Resolved>;
pub type Relation = crate::pipeline::asts::core::Relation<crate::pipeline::asts::core::Resolved>;
pub type DomainExpression =
    crate::pipeline::asts::core::DomainExpression<crate::pipeline::asts::core::Resolved>;
pub type Access = crate::pipeline::asts::core::Access<crate::pipeline::asts::core::Resolved>;
pub type Slot = crate::pipeline::asts::core::Slot<crate::pipeline::asts::core::Resolved>;
pub type FunctionApplication =
    crate::pipeline::asts::core::FunctionApplication<crate::pipeline::asts::core::Resolved>;
pub type Enclyph = crate::pipeline::asts::core::Enclyph<crate::pipeline::asts::core::Resolved>;
pub type Record = crate::pipeline::asts::core::Record<crate::pipeline::asts::core::Resolved>;
pub type RecordMember =
    crate::pipeline::asts::core::RecordMember<crate::pipeline::asts::core::Resolved>;
pub type Tuple = crate::pipeline::asts::core::Tuple<crate::pipeline::asts::core::Resolved>;
pub type TreePattern =
    crate::pipeline::asts::core::TreePattern<crate::pipeline::asts::core::Resolved>;
pub type RecordPattern =
    crate::pipeline::asts::core::RecordPattern<crate::pipeline::asts::core::Resolved>;
pub type MetadataGroup =
    crate::pipeline::asts::core::MetadataGroup<crate::pipeline::asts::core::Resolved>;
pub type FunctorCall =
    crate::pipeline::asts::core::FunctorCall<crate::pipeline::asts::core::Resolved>;
pub type SealedCall =
    crate::pipeline::asts::core::SealedCall<crate::pipeline::asts::core::Resolved>;
pub type ScalarRelation =
    crate::pipeline::asts::core::ScalarRelation<crate::pipeline::asts::core::Resolved>;
pub type ScalarizedRelation =
    crate::pipeline::asts::core::ScalarizedRelation<crate::pipeline::asts::core::Resolved>;
pub type WindowSpec =
    crate::pipeline::asts::core::WindowSpec<crate::pipeline::asts::core::Resolved>;
pub type StandardApplication =
    crate::pipeline::asts::core::StandardApplication<crate::pipeline::asts::core::Resolved>;
pub type TruthExpression =
    crate::pipeline::asts::core::TruthExpression<crate::pipeline::asts::core::Resolved>;
pub type Comparison =
    crate::pipeline::asts::core::Comparison<crate::pipeline::asts::core::Resolved>;
pub type Existence = crate::pipeline::asts::core::Existence<crate::pipeline::asts::core::Resolved>;
pub type Membership =
    crate::pipeline::asts::core::Membership<crate::pipeline::asts::core::Resolved>;
pub type SigmaApplication =
    crate::pipeline::asts::core::SigmaApplication<crate::pipeline::asts::core::Resolved>;
pub type ArgumentValue =
    crate::pipeline::asts::core::ArgumentValue<crate::pipeline::asts::core::Resolved>;
pub type CaseExpression =
    crate::pipeline::asts::core::expressions::CaseExpression<crate::pipeline::asts::core::Resolved>;
pub type ValueTemplate =
    crate::pipeline::asts::core::expressions::ValueTemplate<crate::pipeline::asts::core::Resolved>;
pub type ValueTemplatePart = crate::pipeline::asts::core::expressions::ValueTemplatePart<
    crate::pipeline::asts::core::Resolved,
>;
pub type PipeOp = crate::pipeline::asts::core::PipeOp<crate::pipeline::asts::core::Resolved>;
pub type GroupSpec = crate::pipeline::asts::core::GroupSpec<crate::pipeline::asts::core::Resolved>;
pub type OrderingSpec =
    crate::pipeline::asts::core::OrderingSpec<crate::pipeline::asts::core::Resolved>;
pub type OutItem = crate::pipeline::asts::core::OutItem<crate::pipeline::asts::core::Resolved>;
pub type ReductionItem =
    crate::pipeline::asts::core::ReductionItem<crate::pipeline::asts::core::Resolved>;
pub type RenameSpec =
    crate::pipeline::asts::core::RenameSpec<crate::pipeline::asts::core::Resolved>;
pub type RepositionSpec =
    crate::pipeline::asts::core::RepositionSpec<crate::pipeline::asts::core::Resolved>;
pub type ColumnAlias = crate::pipeline::asts::core::operators::ColumnAlias;
pub type SelectorItem =
    crate::pipeline::asts::core::SelectorItem<crate::pipeline::asts::core::Resolved>;
pub type ColumnNameTemplate = crate::pipeline::asts::core::operators::ColumnNameTemplate;
pub type WindowFrame =
    crate::pipeline::asts::core::WindowFrame<crate::pipeline::asts::core::Resolved>;
pub type FrameBound =
    crate::pipeline::asts::core::FrameBound<crate::pipeline::asts::core::Resolved>;
pub type ScalarArgument =
    crate::pipeline::asts::core::operators::ScalarArgument<crate::pipeline::asts::core::Resolved>;
pub type CallArguments =
    crate::pipeline::asts::core::operators::CallArguments<crate::pipeline::asts::core::Resolved>;
pub type CteRequirements = crate::pipeline::asts::core::expressions::CteRequirements<
    crate::pipeline::asts::core::Resolved,
>;
pub type ReductionPlan =
    crate::pipeline::asts::core::ReductionPlan<crate::pipeline::asts::core::Resolved>;
pub type TreeGroupPlan =
    crate::pipeline::asts::core::TreeGroupPlan<crate::pipeline::asts::core::Resolved>;

// Re-export non-parameterized types from core
pub use crate::pipeline::asts::core::expressions::{
    InnerRelationPattern, NestedMemberCteInfo, TreeGroupLocation,
};
pub use crate::pipeline::asts::core::{
    // Resolution-specific types
    ColumnMetadata,
    // Supporting types
    CteOrigin,
    FilterOrigin,
    LiteralValue,
    NamespacePath,
    OrderDirection,
    QualifiedName,
    Resolved,
    SetOperator,
    TupleOrdinalClause,
    TupleOrdinalOperator,
};
