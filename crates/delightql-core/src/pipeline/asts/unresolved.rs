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
pub type PrecompiledCfeDefinition = crate::pipeline::asts::core::PrecompiledCfeDefinition;
pub type ErContextSpec = crate::pipeline::asts::core::ErContextSpec;
pub type RelationalExpression =
    crate::pipeline::asts::core::RelationalExpression<crate::pipeline::asts::core::Unresolved>;
pub type Relation = crate::pipeline::asts::core::Relation<crate::pipeline::asts::core::Unresolved>;
pub type PipeExpression =
    crate::pipeline::asts::core::PipeExpression<crate::pipeline::asts::core::Unresolved>;
pub type SigmaCondition =
    crate::pipeline::asts::core::SigmaCondition<crate::pipeline::asts::core::Unresolved>;
pub type DomainExpression =
    crate::pipeline::asts::core::DomainExpression<crate::pipeline::asts::core::Unresolved>;
pub type ProjectionExpr =
    crate::pipeline::asts::core::ProjectionExpr<crate::pipeline::asts::core::Unresolved>;
pub type DomainSpec =
    crate::pipeline::asts::core::DomainSpec<crate::pipeline::asts::core::Unresolved>;
pub type FunctionExpression =
    crate::pipeline::asts::core::FunctionExpression<crate::pipeline::asts::core::Unresolved>;
pub type CurlyMember =
    crate::pipeline::asts::core::CurlyMember<crate::pipeline::asts::core::Unresolved>;
pub type ArrayMember =
    crate::pipeline::asts::core::ArrayMember<crate::pipeline::asts::core::Unresolved>;
pub type BooleanExpression =
    crate::pipeline::asts::core::BooleanExpression<crate::pipeline::asts::core::Unresolved>;
pub type UnaryRelationalOperator =
    crate::pipeline::asts::core::UnaryRelationalOperator<crate::pipeline::asts::core::Unresolved>;
pub type ModuloSpec =
    crate::pipeline::asts::core::ModuloSpec<crate::pipeline::asts::core::Unresolved>;
pub type OrderingSpec =
    crate::pipeline::asts::core::OrderingSpec<crate::pipeline::asts::core::Unresolved>;
pub type RenameSpec =
    crate::pipeline::asts::core::RenameSpec<crate::pipeline::asts::core::Unresolved>;
pub type RenameTarget = crate::pipeline::asts::core::RenameTarget;
pub type RepositionSpec =
    crate::pipeline::asts::core::RepositionSpec<crate::pipeline::asts::core::Unresolved>;
pub type ColumnSelector =
    crate::pipeline::asts::core::operators::ColumnSelector<crate::pipeline::asts::core::Unresolved>;
pub type ColumnAlias = crate::pipeline::asts::core::operators::ColumnAlias;
pub type ColumnNameTemplate = crate::pipeline::asts::core::operators::ColumnNameTemplate;
pub type Row = crate::pipeline::asts::core::Row<crate::pipeline::asts::core::Unresolved>;
pub type WindowFrame = crate::pipeline::asts::core::WindowFrame;
pub type FrameMode = crate::pipeline::asts::core::FrameMode;
pub type FrameBound = crate::pipeline::asts::core::FrameBound;
pub type HoArgument =
    crate::pipeline::asts::core::operators::HoArgument<crate::pipeline::asts::core::Unresolved>;

// Re-export non-parameterized types from core
pub use crate::pipeline::asts::core::expressions::{
    CaseArm, InnerRelationPattern, StringTemplatePart,
};
pub use crate::pipeline::asts::core::metadata::{GroundedPath, NamespacePath};
pub use crate::pipeline::asts::core::{
    AssertionPredicate, AssertionSpec, ColumnOrdinal, ColumnRange, ContainmentSemantic,
    ContextMode, DangerSpec, DestructureMode, EmitSpec, InlineDdlSpec, LiteralValue, OptionSpec,
    OrderDirection, PhaseBox, PhaseBoxable, QualifiedName, SetOperator, TupleOrdinalClause,
    TupleOrdinalOperator, UsingColumn,
};

// ============================================================================
// Top-Level Query Structure
// ============================================================================

// Query is now re-exported from core

// ============================================================================
// Relational Expressions (Primary Query Structure)
// ============================================================================

// RelationalExpression is now re-exported from core

// Relation is now re-exported from core

// AndExpression was removed - And pattern is now eliminated

// SigmaCondition is now re-exported from core

// PipeExpression is now re-exported from core

// ============================================================================
// Unary Relational Operators (Pipe Operations)
// ============================================================================

// UnaryRelationalOperator is now in core.rs

// ============================================================================
// Domain Expressions (Values and Computations)
// ============================================================================

// ============================================================================
// Lispy Display Implementation
// ============================================================================

// ============================================================================
// Builder convenience methods
// ============================================================================

impl Query {
    /// Create a simple table query
    pub fn table(name: &str) -> Self {
        Query::Relational(RelationalExpression::Relation(Relation::Ground {
            identifier: QualifiedName {
                namespace_path: NamespacePath::empty(),
                name: name.into(),
                grounding: None,
            },
            canonical_name: PhaseBox::phantom(),
            domain_spec: DomainSpec::Glob,
            alias: None,
            outer: false,
            mutation_target: false,
            passthrough: false,
            cpr_schema: PhaseBox::phantom(),
            hygienic_injections: Vec::new(),
        }))
    }
}

impl RelationalExpression {
    /// Add an alias to a ground relation
    pub fn with_alias(mut self, alias: &str) -> Self {
        if let RelationalExpression::Relation(Relation::Ground {
            alias: ref mut a, ..
        }) = self
        {
            *a = Some(alias.into());
        }
        self
    }

    /// Create a join
    pub fn join(self, other: RelationalExpression) -> Self {
        RelationalExpression::Join {
            left: Box::new(self),
            right: Box::new(other),
            join_condition: None,
            join_type: None,
            cpr_schema: PhaseBox::phantom(),
        }
    }

    /// Add a sigma condition
    pub fn where_condition(self, condition: BooleanExpression) -> Self {
        // Now directly use BooleanExpression
        let sigma_condition = SigmaCondition::Predicate(condition);

        RelationalExpression::Filter {
            source: Box::new(self),
            condition: sigma_condition,
            origin: crate::pipeline::asts::core::FilterOrigin::UserWritten,
            cpr_schema: PhaseBox::phantom(),
        }
    }

    pub fn pipe(self, operator: UnaryRelationalOperator) -> Self {
        RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
            source: self,
            operator,
            cpr_schema: PhaseBox::phantom(),
        })))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_query() {
        let query = Query::table("users");
        assert!(matches!(query, Query::Relational(_)));
    }

    #[test]
    fn test_query_with_alias() {
        let query = Query::Relational(
            RelationalExpression::Relation(Relation::Ground {
                identifier: QualifiedName {
                    namespace_path: NamespacePath::empty(),
                    name: "users".to_string().into(),
                    grounding: None,
                },
                canonical_name: PhaseBox::phantom(),
                domain_spec: DomainSpec::Glob,
                alias: None,
                outer: false,
                mutation_target: false,
                passthrough: false,
                cpr_schema: PhaseBox::phantom(),
                hygienic_injections: Vec::new(),
            })
            .with_alias("u"),
        );

        if let Query::Relational(RelationalExpression::Relation(Relation::Ground {
            alias, ..
        })) = query
        {
            assert_eq!(alias, Some("u".to_string().into()));
        } else {
            panic!("Expected ground relation");
        }
    }

    #[test]
    fn test_outer_join() {
        let query = Query::Relational(RelationalExpression::Relation(Relation::Ground {
            identifier: QualifiedName {
                namespace_path: NamespacePath::empty(),
                name: "orders".to_string().into(),
                grounding: None,
            },
            canonical_name: PhaseBox::phantom(),
            domain_spec: DomainSpec::Glob,
            alias: Some("o".to_string().into()),
            outer: true,
            mutation_target: false,
            passthrough: false,
            cpr_schema: PhaseBox::phantom(),
            hygienic_injections: Vec::new(),
        }));

        if let Query::Relational(RelationalExpression::Relation(Relation::Ground {
            outer, ..
        })) = query
        {
            assert!(outer);
        } else {
            panic!("Expected ground relation");
        }
    }
}
