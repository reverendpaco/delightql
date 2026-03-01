//! Domain, Boolean, Function, and Relational expressions
//!
//! This module contains all expression types for the AST, organized by category:
//! - domain: DomainExpression, DomainSpec
//! - boolean: BooleanExpression
//! - functions: FunctionExpression and helpers (CurlyMember, CaseArm, etc.)
//! - relational: RelationalExpression, Relation, InnerRelationPattern
//! - pipes: PipeExpression, SigmaCondition
//! - metadata_types: FilterOrigin, SetOperator, TreeGroupLocation, CteRequirements
//! - helpers: QualifiedName, UsingColumn

pub mod boolean;
pub mod domain;
pub mod functions;
pub mod helpers;
pub mod metadata_types;
pub mod pipes;
pub mod relational;

// Re-export all public types for backward compatibility
pub use boolean::BooleanExpression;
pub use domain::{DomainExpression, DomainSpec, ProjectionExpr, SubstitutionExpr};
pub use functions::{ArrayMember, CaseArm, CurlyMember, FunctionExpression, StringTemplatePart};
pub use helpers::{QualifiedName, UsingColumn};
pub use metadata_types::{
    CteRequirements, FilterOrigin, NestedMemberCteInfo, SetOperator, TreeGroupLocation,
};
pub use pipes::{DestructureMapping, DestructureMode, PipeExpression, SigmaCondition};
pub use relational::{InnerRelationPattern, Relation, RelationalExpression};
