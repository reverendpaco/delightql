// ast_resolved.rs - Semantically resolved AST for DelightQL
//
// This module defines the resolved (semantic) AST that comes from
// the resolver phase. It contains the same structure as ast_unresolved
// but with added semantic information (CprSchema) on relation-producing nodes.
//
// Key additions over ast_unresolved:
// 1. CprSchema enum for tracking column resolution state
// 2. ColumnMetadata for rich column information
// 3. cpr_schema fields on all relation-producing nodes

// Type aliases for resolved phase
pub type Query = crate::pipeline::asts::core::Query<crate::pipeline::asts::core::Resolved>;
pub type CteBinding =
    crate::pipeline::asts::core::CteBinding<crate::pipeline::asts::core::Resolved>;
pub type RelationalExpression =
    crate::pipeline::asts::core::RelationalExpression<crate::pipeline::asts::core::Resolved>;
pub type Relation = crate::pipeline::asts::core::Relation<crate::pipeline::asts::core::Resolved>;
pub type PipeExpression =
    crate::pipeline::asts::core::PipeExpression<crate::pipeline::asts::core::Resolved>;
pub type SigmaCondition =
    crate::pipeline::asts::core::SigmaCondition<crate::pipeline::asts::core::Resolved>;
pub type DomainExpression =
    crate::pipeline::asts::core::DomainExpression<crate::pipeline::asts::core::Resolved>;
pub type DomainSpec =
    crate::pipeline::asts::core::DomainSpec<crate::pipeline::asts::core::Resolved>;
pub type FunctionExpression =
    crate::pipeline::asts::core::FunctionExpression<crate::pipeline::asts::core::Resolved>;
pub type CurlyMember =
    crate::pipeline::asts::core::CurlyMember<crate::pipeline::asts::core::Resolved>;
pub type ArrayMember =
    crate::pipeline::asts::core::ArrayMember<crate::pipeline::asts::core::Resolved>;
pub type BooleanExpression =
    crate::pipeline::asts::core::BooleanExpression<crate::pipeline::asts::core::Resolved>;
pub type CaseArm =
    crate::pipeline::asts::core::expressions::CaseArm<crate::pipeline::asts::core::Resolved>;
pub type UnaryRelationalOperator =
    crate::pipeline::asts::core::UnaryRelationalOperator<crate::pipeline::asts::core::Resolved>;
pub type ModuloSpec =
    crate::pipeline::asts::core::ModuloSpec<crate::pipeline::asts::core::Resolved>;
pub type OrderingSpec =
    crate::pipeline::asts::core::OrderingSpec<crate::pipeline::asts::core::Resolved>;
pub type RenameSpec =
    crate::pipeline::asts::core::RenameSpec<crate::pipeline::asts::core::Resolved>;
pub type RenameTarget = crate::pipeline::asts::core::RenameTarget;
pub type RepositionSpec =
    crate::pipeline::asts::core::RepositionSpec<crate::pipeline::asts::core::Resolved>;
pub type ColumnSelector =
    crate::pipeline::asts::core::operators::ColumnSelector<crate::pipeline::asts::core::Resolved>;
pub type ColumnAlias = crate::pipeline::asts::core::operators::ColumnAlias;
pub type ColumnNameTemplate = crate::pipeline::asts::core::operators::ColumnNameTemplate;
pub type Row = crate::pipeline::asts::core::Row<crate::pipeline::asts::core::Resolved>;
pub type WindowFrame =
    crate::pipeline::asts::core::WindowFrame<crate::pipeline::asts::core::Resolved>;
pub type FrameBound =
    crate::pipeline::asts::core::FrameBound<crate::pipeline::asts::core::Resolved>;
pub type CprSchema = crate::pipeline::asts::core::CprSchema;
pub type CteRequirements = crate::pipeline::asts::core::expressions::CteRequirements<
    crate::pipeline::asts::core::Resolved,
>;

// Re-export non-parameterized types from core
pub use crate::pipeline::asts::core::expressions::{
    InnerRelationPattern, NestedMemberCteInfo, StringTemplatePart, TreeGroupLocation,
};
pub use crate::pipeline::asts::core::{
    ColumnIdentity,
    // Resolution-specific types
    ColumnMetadata,
    ColumnProvenance,
    // Supporting types
    ContainmentSemantic,
    DestructureMapping,
    FilterOrigin,
    FqTable,
    IdentityContext,
    LiteralValue,
    NamespacePath,
    OrderDirection,
    PhaseBox,
    QualifiedName,
    Resolved,
    ScopedSchema,
    SetOperator,
    TableName,
    TransformationPhase,
    TupleOrdinalClause,
    TupleOrdinalOperator,
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

    // Test removed: Query::table helper was creating invalid states.
    // ast_resolved structures should only be created by the resolver.
}
