// ast_addressed.rs - Addressed AST for DelightQL
//
// This module defines the addressed AST that comes from
// the addresser phase. Currently an identity transform from ast_refined.

// Type aliases for addressed phase
pub type Query = crate::pipeline::asts::core::Query<crate::pipeline::asts::core::Addressed>;
pub type CteBinding =
    crate::pipeline::asts::core::CteBinding<crate::pipeline::asts::core::Addressed>;
pub type RelationalExpression =
    crate::pipeline::asts::core::RelationalExpression<crate::pipeline::asts::core::Addressed>;
pub type Relation = crate::pipeline::asts::core::Relation<crate::pipeline::asts::core::Addressed>;
pub type PipeExpression =
    crate::pipeline::asts::core::PipeExpression<crate::pipeline::asts::core::Addressed>;
pub type SigmaCondition =
    crate::pipeline::asts::core::SigmaCondition<crate::pipeline::asts::core::Addressed>;
pub type DomainExpression =
    crate::pipeline::asts::core::DomainExpression<crate::pipeline::asts::core::Addressed>;
pub type ProjectionExpr =
    crate::pipeline::asts::core::ProjectionExpr<crate::pipeline::asts::core::Addressed>;
pub type DomainSpec =
    crate::pipeline::asts::core::DomainSpec<crate::pipeline::asts::core::Addressed>;
pub type FunctionExpression =
    crate::pipeline::asts::core::FunctionExpression<crate::pipeline::asts::core::Addressed>;
pub type CurlyMember =
    crate::pipeline::asts::core::CurlyMember<crate::pipeline::asts::core::Addressed>;
pub type ArrayMember =
    crate::pipeline::asts::core::ArrayMember<crate::pipeline::asts::core::Addressed>;
pub type BooleanExpression =
    crate::pipeline::asts::core::BooleanExpression<crate::pipeline::asts::core::Addressed>;
pub type UnaryRelationalOperator =
    crate::pipeline::asts::core::UnaryRelationalOperator<crate::pipeline::asts::core::Addressed>;
pub type ModuloSpec =
    crate::pipeline::asts::core::ModuloSpec<crate::pipeline::asts::core::Addressed>;
pub type OrderingSpec =
    crate::pipeline::asts::core::OrderingSpec<crate::pipeline::asts::core::Addressed>;
pub type RenameSpec =
    crate::pipeline::asts::core::RenameSpec<crate::pipeline::asts::core::Addressed>;
pub type RepositionSpec =
    crate::pipeline::asts::core::RepositionSpec<crate::pipeline::asts::core::Addressed>;
pub type ColumnSelector =
    crate::pipeline::asts::core::operators::ColumnSelector<crate::pipeline::asts::core::Addressed>;
pub type ColumnAlias = crate::pipeline::asts::core::operators::ColumnAlias;
pub type Row = crate::pipeline::asts::core::Row<crate::pipeline::asts::core::Addressed>;
pub type WindowFrame =
    crate::pipeline::asts::core::WindowFrame<crate::pipeline::asts::core::Addressed>;
pub type FrameMode = crate::pipeline::asts::core::FrameMode;
pub type FrameBound =
    crate::pipeline::asts::core::FrameBound<crate::pipeline::asts::core::Addressed>;
pub type CaseArm =
    crate::pipeline::asts::core::expressions::CaseArm<crate::pipeline::asts::core::Addressed>;
pub type InnerRelationPattern = crate::pipeline::asts::core::expressions::InnerRelationPattern<
    crate::pipeline::asts::core::Addressed,
>;
pub type StringTemplatePart = crate::pipeline::asts::core::expressions::StringTemplatePart<
    crate::pipeline::asts::core::Addressed,
>;
pub type CprSchema = crate::pipeline::asts::core::CprSchema;
pub type PrecompiledCfeDefinition = crate::pipeline::asts::core::PrecompiledCfeDefinition;
pub type CteRequirements = crate::pipeline::asts::core::expressions::CteRequirements<
    crate::pipeline::asts::core::Addressed,
>;

pub use crate::pipeline::asts::core::expressions::TreeGroupLocation;
pub use crate::pipeline::asts::core::{
    Addressed,
    // Resolution-specific types
    ColumnMetadata,
    DestructureMapping,
    DestructureMode,
    FilterOrigin,
    JoinType,
    LiteralValue,
    NamespacePath,
    OrderDirection,
    PhaseBox,
    SetOperator,
    TableName,
    TupleOrdinalOperator,
    UsingColumn,
};
