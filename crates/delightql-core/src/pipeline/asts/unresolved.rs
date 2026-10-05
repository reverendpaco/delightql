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
pub type Chain = crate::pipeline::asts::core::Chain<crate::pipeline::asts::core::Unresolved>;
pub type GroundForm =
    crate::pipeline::asts::core::GroundForm<crate::pipeline::asts::core::Unresolved>;
pub type Step = crate::pipeline::asts::core::Step<crate::pipeline::asts::core::Unresolved>;
#[allow(dead_code)]
pub type Peel = crate::pipeline::asts::core::Peel<crate::pipeline::asts::core::Unresolved>;
#[allow(dead_code)]
#[allow(dead_code)]
pub type Transparent =
    crate::pipeline::asts::core::Transparent<crate::pipeline::asts::core::Unresolved>;
pub type Continuation =
    crate::pipeline::asts::core::Continuation<crate::pipeline::asts::core::Unresolved>;
pub type AnonTable =
    crate::pipeline::asts::core::AnonTable<crate::pipeline::asts::core::Unresolved>;
pub type AnonRelation =
    crate::pipeline::asts::core::AnonRelation<crate::pipeline::asts::core::Unresolved>;
pub type TabularBody<H, D> = crate::pipeline::asts::core::TabularBody<H, D>;
pub type Relation = crate::pipeline::asts::core::Relation<crate::pipeline::asts::core::Unresolved>;
pub type DomainExpression =
    crate::pipeline::asts::core::DomainExpression<crate::pipeline::asts::core::Unresolved>;
pub type Access = crate::pipeline::asts::core::Access<crate::pipeline::asts::core::Unresolved>;
pub type Slot = crate::pipeline::asts::core::Slot<crate::pipeline::asts::core::Unresolved>;
pub type FunctionApplication =
    crate::pipeline::asts::core::FunctionApplication<crate::pipeline::asts::core::Unresolved>;
pub type FunctorCall =
    crate::pipeline::asts::core::FunctorCall<crate::pipeline::asts::core::Unresolved>;
pub type TruthExpression =
    crate::pipeline::asts::core::TruthExpression<crate::pipeline::asts::core::Unresolved>;
pub type PipeOp = crate::pipeline::asts::core::PipeOp<crate::pipeline::asts::core::Unresolved>;
pub type GroupSpec =
    crate::pipeline::asts::core::GroupSpec<crate::pipeline::asts::core::Unresolved>;
pub type ReductionPlan =
    crate::pipeline::asts::core::ReductionPlan<crate::pipeline::asts::core::Unresolved>;
pub type WindowFrame = crate::pipeline::asts::core::WindowFrame;
pub type FrameBound = crate::pipeline::asts::core::FrameBound;

// Re-export non-parameterized types from core
pub use crate::pipeline::asts::core::expressions::InnerRelationPattern;
pub type CaseExpression = crate::pipeline::asts::core::expressions::CaseExpression<
    crate::pipeline::asts::core::Unresolved,
>;
pub use crate::pipeline::asts::core::metadata::NamespacePath;
pub use crate::pipeline::asts::core::{
    DangerSpec, GroundMention, InlineDdlBody, InlineDdlSpec, LiteralValue, OptionSpec,
    QualifiedName, StatementBlocks,
};

impl Chain {
    pub fn pipe(self, operator: PipeOp) -> Self {
        self.then(Step::authored(Continuation::Pipe {
            operator,
            named: None,
        }))
    }
}
