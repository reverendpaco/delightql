// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Domain, Boolean, Function, and Relational expressions
//!
//! This module contains all expression types for the AST, organized by category:
//! - access: Access, Slot
//! - domain: DomainExpression
//! - truth: TruthExpression — the one truth carrier
//! - functions: FunctionApplication and its exact payloads (StandardApplication,
//!   InfixApplication, CaseExpression, ValueTemplate)
//! - enclyph: Enclyph, Record, Tuple, RecordMember — VALUE construction
//! - patterns: TreePattern and its members — the destructuring mirror
//! - metadata_group: MetadataGroup, MetadataTarget — reduction-position keys
//! - chain: Chain, Grelex, Continuation — the relational spine
//! - relational: Relation, InnerRelationPattern
//! - metadata_types: FilterOrigin, SetOperator, TreeGroupLocation, CteRequirements
//! - helpers: QualifiedName
//! - paths: Path, PathStep, JsonAccess — the reach into a value
//! - references: Reference, NamedReference — addressing a column
//! - spreads: Spread, Glob, RegexSelector, SelectorItem, RenameSource

pub mod access;
pub mod chain;
pub mod domain;
pub mod enclyph;
pub mod functions;
pub mod helpers;
pub mod metadata_group;
pub mod metadata_types;
pub mod paths;
pub mod patterns;
pub mod pipes;
pub mod references;
pub mod relational;
pub mod spreads;
pub mod truth;

// The expression vocabulary is ONE vocabulary; the file split is for
// authoring, and a consumer names a type, not the file it was written in.
pub use access::{Access, Slot};
pub use chain::{
    AnonRelation, AnonTable, BagCorrelation, Chain, Continuation, CorrPred, Correspondence, Datum,
    ErJoinStep, Grelex, HeaderItem, MemberCorrelation, StructuralForm, StructuralStep, TabularBody,
    TabularRow, WholeHeading,
};
pub use domain::{DomainExpression, DomainHole};
pub use enclyph::{Enclyph, Record, RecordMember, Tuple};
pub use functions::{
    Callable, CaseExpression, FactFunctionArm, FactFunctionMode, FieldSelect, FunctionApplication,
    FunctorCall, InfixApplication, Lambda, MatchArm, ModeWitness, PureCall, ScalarRelation,
    Scalarization, ScalarizedRelation, SealedCall, SearchedArm, StandardApplication, ValueTemplate,
    ValueTemplatePart, WindowSpec,
};
pub use helpers::QualifiedName;
pub use metadata_group::{MetadataGroup, MetadataTarget};
pub use metadata_types::{
    CteRequirements, FilterOrigin, NestedMemberCteInfo, ReductionPlan, SetOperator,
    TreeGroupLocation, TreeGroupPlan,
};
pub use paths::{JsonAccess, Path, PathStep};
pub use patterns::{
    ArrayPattern, ArrayPatternMember, PathBinding, PatternTarget, RecordPattern,
    RecordPatternMember, TreePattern,
};
pub use pipes::{DestructureMapping, DestructureMode};
pub use references::{NamedReference, Reference};
pub use relational::{GroundMention, InnerRelationPattern, Relation};
pub use spreads::{Glob, RegexSelector, RenameSource, SelectorItem, Spread};
pub use truth::{
    ArgumentValue, Comparison, Existence, Membership, MembershipSource, OutValue, Polarity, Probe,
    ProbeAddressing, RelationalMembership, SigmaApplication, SlotConstraint, TruthAsValue,
    TruthExpression, ValueRow,
};
