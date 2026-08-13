// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
pub mod columns;
pub mod definitions;
pub mod expressions;
pub mod literals;
pub mod metadata;
pub mod operators;
pub mod phases;
pub mod provenance;
pub mod queries;
pub mod smart_constructors;
pub mod specs;

pub use columns::{AtSign, AuthoredColumn, ColumnOccurrence, ContextMarker, WrittenBinder};
pub use expressions::functions::{ClauseArm, ClauseSelection};
pub use expressions::truth::NamedProof;
pub use expressions::{
    Access, AnonRelation, AnonTable, ArgumentValue, ArrayPattern, ArrayPatternMember,
    BagCorrelation, Callable, CaseExpression, Chain, Comparison, Continuation, CorrPred,
    Correspondence, Datum, DestructureMapping, DestructureMode, DomainExpression, DomainHole,
    Enclyph, ErJoinStep, Existence, FactFunctionArm, FactFunctionMode, FieldSelect, FilterOrigin,
    FunctionApplication, FunctorCall, Glob, Grelex, GroundMention, HeaderItem, InfixApplication,
    JsonAccess, Lambda, MatchArm, MemberCorrelation, Membership, MembershipSource, MetadataGroup,
    MetadataTarget, ModeWitness, NamedReference, OutValue, Path, PathBinding, PathStep,
    PatternTarget, Polarity, Probe, ProbeAddressing, PureCall, QualifiedName, Record, RecordMember,
    RecordPattern, RecordPatternMember, ReductionPlan, Reference, RegexSelector, Relation,
    RelationalMembership, RenameSource, ScalarRelation, Scalarization, ScalarizedRelation,
    SealedCall, SearchedArm, SelectorItem, SetOperator, SigmaApplication, Slot, SlotConstraint,
    Spread, StandardApplication, StructuralForm, StructuralStep, TabularBody, TabularRow,
    TreeGroupLocation, TreeGroupPlan, TreePattern, TruthAsValue, TruthExpression, Tuple, ValueRow,
    ValueTemplate, ValueTemplatePart, WholeHeading, WindowSpec,
};
pub use literals::{ColumnOrdinal, ColumnRange, LiteralValue};
pub use metadata::{ColumnMetadata, NamespacePath};
pub use operators::{FrameBound, JoinType, PipeOp, WindowFrame};
pub use phases::{Phase, Refined, Resolved, Unresolved};
pub use provenance::CteOrigin;
pub use queries::{
    AssertionSpec, CfeDefinition, CfeFormals, ContextMode, CteBinding,
    CteAuthority, CteEffectDeclaration, CteSubject, DangerSpec, DangerState, ErContextSpec, InlineDdlBody,
    InlineDdlSpec, OptionSpec, OptionState, Query,
};
pub use specs::{
    DelegateSpec, GroupSpec, MetadataOut, NameTarget, NamedOutItem, OneOut, OrderDirection,
    OrderingSpec, OutItem, PivotSpec, ReductionItem, RenameSpec, RepositionSpec,
    TupleOrdinalClause, TupleOrdinalOperator,
};
