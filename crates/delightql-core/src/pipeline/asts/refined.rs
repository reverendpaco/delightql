// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// ast_refined.rs - Refined AST for DelightQL
//
// This module defines the refined AST that comes from
// the refiner phase. Currently an identity transform from ast_resolved,
// but will eventually handle additional refinements and optimizations.
//
// Key additions over ast_unresolved:
// 1. a scope on every relation-producing node, naming what it publishes
// 2. settled correlations and chosen strategies

// Type aliases for refined phase
pub type Query = crate::pipeline::asts::core::Query<crate::pipeline::asts::core::Refined>;
pub type CteBinding = crate::pipeline::asts::core::CteBinding<crate::pipeline::asts::core::Refined>;
pub type Chain = crate::pipeline::asts::core::Chain<crate::pipeline::asts::core::Refined>;
pub type GroundForm = crate::pipeline::asts::core::GroundForm<crate::pipeline::asts::core::Refined>;
#[allow(dead_code)]
pub type Step = crate::pipeline::asts::core::Step<crate::pipeline::asts::core::Refined>;
#[allow(dead_code)]
pub type Peel = crate::pipeline::asts::core::Peel<crate::pipeline::asts::core::Refined>;
#[allow(dead_code)]
#[allow(dead_code)]
pub type Transparent =
    crate::pipeline::asts::core::Transparent<crate::pipeline::asts::core::Refined>;
pub type Continuation =
    crate::pipeline::asts::core::Continuation<crate::pipeline::asts::core::Refined>;
pub type StructuralForm =
    crate::pipeline::asts::core::StructuralForm<crate::pipeline::asts::core::Refined>;
pub type AnonTable = crate::pipeline::asts::core::AnonTable<crate::pipeline::asts::core::Refined>;
pub type AnonRelation =
    crate::pipeline::asts::core::AnonRelation<crate::pipeline::asts::core::Refined>;
pub type Datum = crate::pipeline::asts::core::Datum<crate::pipeline::asts::core::Refined>;
pub type TabularRow<T> = crate::pipeline::asts::core::TabularRow<T>;
pub type WholeHeading =
    crate::pipeline::asts::core::WholeHeading<crate::pipeline::asts::core::Refined>;
pub type CorrPred = crate::pipeline::asts::core::CorrPred<crate::pipeline::asts::core::Refined>;
pub type MemberCorrelation =
    crate::pipeline::asts::core::MemberCorrelation<crate::pipeline::asts::core::Refined>;
pub type BagCorrelation =
    crate::pipeline::asts::core::BagCorrelation<crate::pipeline::asts::core::Refined>;
pub type Relation = crate::pipeline::asts::core::Relation<crate::pipeline::asts::core::Refined>;
pub type DomainExpression =
    crate::pipeline::asts::core::DomainExpression<crate::pipeline::asts::core::Refined>;
pub type FunctionApplication =
    crate::pipeline::asts::core::FunctionApplication<crate::pipeline::asts::core::Refined>;
pub type TreePattern =
    crate::pipeline::asts::core::TreePattern<crate::pipeline::asts::core::Refined>;
pub type RecordPattern =
    crate::pipeline::asts::core::RecordPattern<crate::pipeline::asts::core::Refined>;
pub type MetadataGroup =
    crate::pipeline::asts::core::MetadataGroup<crate::pipeline::asts::core::Refined>;
pub type FunctorCall =
    crate::pipeline::asts::core::FunctorCall<crate::pipeline::asts::core::Refined>;
pub type SealedCall = crate::pipeline::asts::core::SealedCall<crate::pipeline::asts::core::Refined>;
pub type PureCall = crate::pipeline::asts::core::PureCall<crate::pipeline::asts::core::Refined>;
pub type Callable = crate::pipeline::asts::core::Callable<crate::pipeline::asts::core::Refined>;
pub type Access = crate::pipeline::asts::core::Access<crate::pipeline::asts::core::Refined>;
pub type TruthExpression =
    crate::pipeline::asts::core::TruthExpression<crate::pipeline::asts::core::Refined>;
pub type ArgumentValue =
    crate::pipeline::asts::core::ArgumentValue<crate::pipeline::asts::core::Refined>;
pub type OrderingSpec =
    crate::pipeline::asts::core::OrderingSpec<crate::pipeline::asts::core::Refined>;
pub type WindowFrame =
    crate::pipeline::asts::core::WindowFrame<crate::pipeline::asts::core::Refined>;
pub type CaseExpression =
    crate::pipeline::asts::core::expressions::CaseExpression<crate::pipeline::asts::core::Refined>;
pub type GroupSpec = crate::pipeline::asts::core::GroupSpec<crate::pipeline::asts::core::Refined>;
pub type ReductionPlan =
    crate::pipeline::asts::core::ReductionPlan<crate::pipeline::asts::core::Refined>;
pub type DelegateSpec =
    crate::pipeline::asts::core::DelegateSpec<crate::pipeline::asts::core::Refined>;
pub type OutItem = crate::pipeline::asts::core::OutItem<crate::pipeline::asts::core::Refined>;
pub type ReductionItem =
    crate::pipeline::asts::core::ReductionItem<crate::pipeline::asts::core::Refined>;
pub type Reference = crate::pipeline::asts::core::Reference<crate::pipeline::asts::core::Refined>;
pub type NamedOutItem =
    crate::pipeline::asts::core::NamedOutItem<crate::pipeline::asts::core::Refined>;
pub type AppliedCell =
    crate::pipeline::asts::core::operators::AppliedCell<crate::pipeline::asts::core::Refined>;
pub type RenameSpec = crate::pipeline::asts::core::RenameSpec<crate::pipeline::asts::core::Refined>;
pub type SelectorItem =
    crate::pipeline::asts::core::SelectorItem<crate::pipeline::asts::core::Refined>;
pub type Path = crate::pipeline::asts::core::Path;
pub type PathStep = crate::pipeline::asts::core::PathStep;
pub type ScalarArgument =
    crate::pipeline::asts::core::operators::ScalarArgument<crate::pipeline::asts::core::Refined>;
pub type InnerRelationPattern = crate::pipeline::asts::core::expressions::InnerRelationPattern<
    crate::pipeline::asts::core::Refined,
>;
pub use crate::pipeline::asts::core::{
    DestructureMapping, DestructureMode, FilterOrigin, JoinType, LiteralValue, OrderDirection,
    Refined, SetOperator, TreeGroupLocation,
};
