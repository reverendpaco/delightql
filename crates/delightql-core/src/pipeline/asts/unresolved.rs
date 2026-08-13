// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// ast_unresolved.rs - Pure syntactic AST for DelightQL (NEW PROPOSAL)
//
// This module defines the unresolved (syntactic) AST that comes directly from
// the builder phase. It contains NO semantic information - only syntax structure.
//
// Based on analysis of 64 builder_output sketches, this design captures:
// 1. Pure syntactic structure with no semantic markers
// 2. Clean separation of relations, operators, and expressions
// 3. Support for all DelightQL syntactic features
// 4. No Incomplete/Resolved variants - those belong in later phases

// Type aliases for unresolved phase
pub type Query = crate::pipeline::asts::core::Query<crate::pipeline::asts::core::Unresolved>;
pub type CteBinding =
    crate::pipeline::asts::core::CteBinding<crate::pipeline::asts::core::Unresolved>;
// CFE definitions are not phase-specific (always unresolved at definition time)
pub type CfeDefinition = crate::pipeline::asts::core::CfeDefinition;
pub type ErContextSpec = crate::pipeline::asts::core::ErContextSpec;
pub type Chain = crate::pipeline::asts::core::Chain<crate::pipeline::asts::core::Unresolved>;
pub type Grelex = crate::pipeline::asts::core::Grelex<crate::pipeline::asts::core::Unresolved>;
pub type WholeHeading =
    crate::pipeline::asts::core::WholeHeading<crate::pipeline::asts::core::Unresolved>;
pub type MemberCorrelation =
    crate::pipeline::asts::core::MemberCorrelation<crate::pipeline::asts::core::Unresolved>;
pub type Continuation =
    crate::pipeline::asts::core::Continuation<crate::pipeline::asts::core::Unresolved>;
pub type StructuralStep =
    crate::pipeline::asts::core::StructuralStep<crate::pipeline::asts::core::Unresolved>;
pub type StructuralForm =
    crate::pipeline::asts::core::StructuralForm<crate::pipeline::asts::core::Unresolved>;
pub type AnonTable =
    crate::pipeline::asts::core::AnonTable<crate::pipeline::asts::core::Unresolved>;
pub type AnonRelation =
    crate::pipeline::asts::core::AnonRelation<crate::pipeline::asts::core::Unresolved>;
pub type Datum = crate::pipeline::asts::core::Datum<crate::pipeline::asts::core::Unresolved>;
pub type HeaderItem =
    crate::pipeline::asts::core::HeaderItem<crate::pipeline::asts::core::Unresolved>;
pub type TabularBody<H, D> = crate::pipeline::asts::core::TabularBody<H, D>;
pub type ErJoinStep =
    crate::pipeline::asts::core::ErJoinStep<crate::pipeline::asts::core::Unresolved>;
pub type Relation = crate::pipeline::asts::core::Relation<crate::pipeline::asts::core::Unresolved>;
pub type DomainExpression =
    crate::pipeline::asts::core::DomainExpression<crate::pipeline::asts::core::Unresolved>;
pub type Access = crate::pipeline::asts::core::Access<crate::pipeline::asts::core::Unresolved>;
pub type Slot = crate::pipeline::asts::core::Slot<crate::pipeline::asts::core::Unresolved>;
pub type FunctionApplication =
    crate::pipeline::asts::core::FunctionApplication<crate::pipeline::asts::core::Unresolved>;
pub type DomainHole = crate::pipeline::asts::core::DomainHole;
pub type Enclyph = crate::pipeline::asts::core::Enclyph<crate::pipeline::asts::core::Unresolved>;
pub type Record = crate::pipeline::asts::core::Record<crate::pipeline::asts::core::Unresolved>;
pub type Tuple = crate::pipeline::asts::core::Tuple<crate::pipeline::asts::core::Unresolved>;
pub type TreePattern =
    crate::pipeline::asts::core::TreePattern<crate::pipeline::asts::core::Unresolved>;
pub type RecordPattern =
    crate::pipeline::asts::core::RecordPattern<crate::pipeline::asts::core::Unresolved>;
pub type MetadataGroup =
    crate::pipeline::asts::core::MetadataGroup<crate::pipeline::asts::core::Unresolved>;
pub type FunctorCall =
    crate::pipeline::asts::core::FunctorCall<crate::pipeline::asts::core::Unresolved>;
pub type SealedCall =
    crate::pipeline::asts::core::SealedCall<crate::pipeline::asts::core::Unresolved>;
pub type PureCall = crate::pipeline::asts::core::PureCall<crate::pipeline::asts::core::Unresolved>;
pub type Callable = crate::pipeline::asts::core::Callable<crate::pipeline::asts::core::Unresolved>;
pub type ScalarRelation =
    crate::pipeline::asts::core::ScalarRelation<crate::pipeline::asts::core::Unresolved>;
pub type StandardApplication =
    crate::pipeline::asts::core::StandardApplication<crate::pipeline::asts::core::Unresolved>;
pub type TruthExpression =
    crate::pipeline::asts::core::TruthExpression<crate::pipeline::asts::core::Unresolved>;
pub type Probe = crate::pipeline::asts::core::Probe<crate::pipeline::asts::core::Unresolved>;
pub type ValueRow = crate::pipeline::asts::core::ValueRow<crate::pipeline::asts::core::Unresolved>;
pub type ArgumentValue =
    crate::pipeline::asts::core::ArgumentValue<crate::pipeline::asts::core::Unresolved>;
pub type OutValue = crate::pipeline::asts::core::OutValue<crate::pipeline::asts::core::Unresolved>;
pub type SlotConstraint =
    crate::pipeline::asts::core::SlotConstraint<crate::pipeline::asts::core::Unresolved>;
pub type PipeOp = crate::pipeline::asts::core::PipeOp<crate::pipeline::asts::core::Unresolved>;
pub type GroupSpec =
    crate::pipeline::asts::core::GroupSpec<crate::pipeline::asts::core::Unresolved>;
pub type ReductionPlan =
    crate::pipeline::asts::core::ReductionPlan<crate::pipeline::asts::core::Unresolved>;
pub type OrderingSpec =
    crate::pipeline::asts::core::OrderingSpec<crate::pipeline::asts::core::Unresolved>;
pub type OutItem = crate::pipeline::asts::core::OutItem<crate::pipeline::asts::core::Unresolved>;
pub type ReductionItem =
    crate::pipeline::asts::core::ReductionItem<crate::pipeline::asts::core::Unresolved>;
pub type Reference =
    crate::pipeline::asts::core::Reference<crate::pipeline::asts::core::Unresolved>;
pub type OneOut = crate::pipeline::asts::core::OneOut<crate::pipeline::asts::core::Unresolved>;
pub type NamedOutItem =
    crate::pipeline::asts::core::NamedOutItem<crate::pipeline::asts::core::Unresolved>;
pub type DelegateSpec =
    crate::pipeline::asts::core::DelegateSpec<crate::pipeline::asts::core::Unresolved>;
pub type RenameSpec =
    crate::pipeline::asts::core::RenameSpec<crate::pipeline::asts::core::Unresolved>;
pub type RenameTarget = crate::pipeline::asts::core::NameTarget;
pub type RepositionSpec =
    crate::pipeline::asts::core::RepositionSpec<crate::pipeline::asts::core::Unresolved>;
pub type ColumnAlias = crate::pipeline::asts::core::operators::ColumnAlias;
pub type SelectorItem =
    crate::pipeline::asts::core::SelectorItem<crate::pipeline::asts::core::Unresolved>;
pub type Spread = crate::pipeline::asts::core::Spread<crate::pipeline::asts::core::Unresolved>;
pub type RenameSource =
    crate::pipeline::asts::core::RenameSource<crate::pipeline::asts::core::Unresolved>;
pub type WindowFrame = crate::pipeline::asts::core::WindowFrame;
pub type FrameBound = crate::pipeline::asts::core::FrameBound;
pub type HoArgument =
    crate::pipeline::asts::core::operators::HoArgument<crate::pipeline::asts::core::Unresolved>;
pub type ScalarArgument =
    crate::pipeline::asts::core::operators::ScalarArgument<crate::pipeline::asts::core::Unresolved>;
pub type CallArguments =
    crate::pipeline::asts::core::operators::CallArguments<crate::pipeline::asts::core::Unresolved>;

// Re-export non-parameterized types from core
pub use crate::pipeline::asts::core::expressions::InnerRelationPattern;
pub type CaseExpression = crate::pipeline::asts::core::expressions::CaseExpression<
    crate::pipeline::asts::core::Unresolved,
>;
pub type ValueTemplatePart = crate::pipeline::asts::core::expressions::ValueTemplatePart<
    crate::pipeline::asts::core::Unresolved,
>;
pub use crate::pipeline::asts::core::metadata::{GroundedPath, NamespacePath};
pub use crate::pipeline::asts::core::{
    AssertionSpec, ColumnOrdinal, ColumnRange, DangerSpec, DestructureMode, GroundMention,
    InlineDdlBody, InlineDdlSpec, LiteralValue, OptionSpec, OrderDirection, QualifiedName,
    SetOperator,
};

impl Chain {
    pub fn pipe(self, operator: PipeOp) -> Self {
        self.then(Continuation::Pipe {
            operator,
            named: None,
            cpr_schema: (),
        })
    }
}
