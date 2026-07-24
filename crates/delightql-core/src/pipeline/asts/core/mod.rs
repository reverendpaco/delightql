// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
pub mod expressions;
pub mod literals;
pub mod metadata;
pub mod operators;
pub mod phase_box;
pub mod phases;
pub mod provenance;
pub mod queries;
pub mod smart_constructors;
pub mod specs;

pub use expressions::{
    ArrayMember, BooleanExpression, CurlyMember, DestructureMapping, DestructureMode,
    DomainExpression, DomainSpec, FilterOrigin, FunctionExpression, PipeExpression, ProjectionExpr,
    QualifiedName, Relation, RelationalExpression, SetOperator, SigmaCondition, SubstitutionExpr,
    UsingColumn,
};
pub use literals::{ColumnOrdinal, ColumnRange, LiteralValue};
pub use metadata::{ColumnMetadata, CprSchema, NamespacePath, ScopedSchema, TableName};
pub use operators::{FrameBound, FrameMode, JoinType, UnaryRelationalOperator, WindowFrame};
pub use phase_box::{PhaseBox, PhaseBoxable};
pub use phases::{Addressed, Refined, Resolved, Unresolved};
pub use provenance::{
    ColumnIdentity, ColumnProvenance, CteOrigin, IdentityContext, QualificationSource, ResolverId,
    TransformationPhase,
};
pub use queries::{
    AssertionPredicate, AssertionSpec, CfeDefinition, ContextMode, CteBinding, DangerSpec,
    DangerState, ErContextSpec, InlineDdlSpec, OptionSpec, OptionState,
    PrecompiledCfeDefinition, Query,
};
pub use specs::{
    ContainmentSemantic, ModuloSpec, OrderDirection, OrderingSpec, OutputDomainExpression,
    RenameSpec, RenameTarget, RepositionSpec, Row, TupleOrdinalClause, TupleOrdinalOperator,
    DelegateSpec,
};
