// ast_refined.rs - Refined AST for DelightQL
//
// This module defines the refined AST that comes from
// the refiner phase. Currently an identity transform from ast_resolved,
// but will eventually handle additional refinements and optimizations.
//
// Key additions over ast_unresolved:
// 1. CprSchema enum for tracking column resolution state
// 2. ColumnMetadata for rich column information
// 3. cpr_schema fields on all relation-producing nodes

// Type aliases for refined phase
pub type Query = crate::pipeline::asts::core::Query<crate::pipeline::asts::core::Refined>;
pub type CteBinding = crate::pipeline::asts::core::CteBinding<crate::pipeline::asts::core::Refined>;
pub type RelationalExpression =
    crate::pipeline::asts::core::RelationalExpression<crate::pipeline::asts::core::Refined>;
pub type Relation = crate::pipeline::asts::core::Relation<crate::pipeline::asts::core::Refined>;
pub type PipeExpression =
    crate::pipeline::asts::core::PipeExpression<crate::pipeline::asts::core::Refined>;
pub type SigmaCondition =
    crate::pipeline::asts::core::SigmaCondition<crate::pipeline::asts::core::Refined>;
pub type DomainExpression =
    crate::pipeline::asts::core::DomainExpression<crate::pipeline::asts::core::Refined>;
pub type FunctionExpression =
    crate::pipeline::asts::core::FunctionExpression<crate::pipeline::asts::core::Refined>;
pub type CurlyMember =
    crate::pipeline::asts::core::CurlyMember<crate::pipeline::asts::core::Refined>;
pub type BooleanExpression =
    crate::pipeline::asts::core::BooleanExpression<crate::pipeline::asts::core::Refined>;
pub type UnaryRelationalOperator =
    crate::pipeline::asts::core::UnaryRelationalOperator<crate::pipeline::asts::core::Refined>;
pub type OrderingSpec =
    crate::pipeline::asts::core::OrderingSpec<crate::pipeline::asts::core::Refined>;
pub type WindowFrame =
    crate::pipeline::asts::core::WindowFrame<crate::pipeline::asts::core::Refined>;
pub type FrameBound = crate::pipeline::asts::core::FrameBound<crate::pipeline::asts::core::Refined>;
pub type CaseArm =
    crate::pipeline::asts::core::expressions::CaseArm<crate::pipeline::asts::core::Refined>;
pub type InnerRelationPattern = crate::pipeline::asts::core::expressions::InnerRelationPattern<
    crate::pipeline::asts::core::Refined,
>;
pub type StringTemplatePart = crate::pipeline::asts::core::expressions::StringTemplatePart<
    crate::pipeline::asts::core::Refined,
>;
pub use crate::pipeline::asts::core::expressions::domain::LvarProvenance;
pub use crate::pipeline::asts::core::{
    ContainmentSemantic,
    JoinType,
    LiteralValue,
    PhaseBox,
    QualifiedName,
    Refined,
    ScopedSchema,
    SetOperator,
    UsingColumn,
};

// ============================================================================
// Resolution Types (NEW in ast_resolved)
// ============================================================================

// ============================================================================
// NOTE: Test helpers removed to prevent invalid states
// ============================================================================
// Test helpers that created ast_resolved structures with fake CprSchema values
// have been removed. The ast_resolved structures should only be created by
// the resolver, not manually constructed with invalid states.
//
// Once the resolver is implemented, tests should:
// 1. Create ast_unresolved structures (which don't have cpr_schema)
// 2. Pass them through the resolver
// 3. Get properly resolved ast_resolved structures
//
// This ensures we're testing real states, not fake ones.

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Tests removed: These were creating invalid states with incorrect field names
    // after the ColumnMetadata refactoring.
}
