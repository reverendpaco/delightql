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
    Correspondence, Crossing, Datum, DestructureMapping, DestructureMode, DomainExpression,
    DomainHole, Enclyph, ErJoinStep, Existence, FactFunctionArm, FactFunctionDefinition,
    FactFunctionMode, FieldSelect, FilterOrigin, FormalHole, FunctionApplication, FunctorCall,
    Glob, Grelex, GroundForm, GroundMention, HeaderItem, InfixApplication, JsonAccess, Lambda,
    MatchArm, MemberCorrelation, Membership, MembershipSource, MetadataGroup, MetadataTarget,
    ModeWitness, NamedReference, Path, PathBinding, PathStep, PatternTarget, Peel, Polarity, Probe,
    ProbeAddressing, PureCall, QualifiedName, Record, RecordMember, RecordPattern,
    RecordPatternMember, ReductionPlan, Reference, RegexSelector, Relation, RelationalMembership,
    RenameSource, RunForm, ScalarRelation, Scalarization, ScalarizedRelation, SealedCall,
    SearchedArm, SelectorItem, SetOperator, SigmaApplication, Slot, Spread, StandardApplication,
    Standing, Step, StructuralForm, StructuralStep, TabularBody, TabularRow, Transparent,
    TreeGroupLocation, TreeGroupPlan, TreePattern, TruthConsumer, TruthExpression, Tuple,
    TupleElement, ValueRow, ValueTemplate, ValueTemplatePart, WholeHeading, WindowSpec,
};
pub use literals::{ColumnOrdinal, ColumnRange, LiteralValue};
pub use metadata::{ColumnMetadata, NamespacePath};
pub use operators::{FrameBound, JoinType, PipeOp, WindowFrame};
pub use phases::{Phase, Refined, Resolved, Unresolved};
pub use provenance::CteOrigin;
pub use queries::{
    AuthoredCteSubject, CfeDefinition, CfeFormals, ContextMode, CteAuthority, CteBinding,
    CteEffectDeclaration, CteSubjectView, DangerSpec, DangerState, ErContextSpec, HoDefinition,
    InlineDdlBody, InlineDdlSpec, LexicalHorizon, OptionSpec, OptionState, Query, QueryLocalBlock,
    QueryLocalNames, QueryLocals,
};
pub(crate) use queries::{QueryLocalDemand, QueryLocalKind};
pub use specs::{
    DelegateSpec, GroupSpec, MetadataOut, NameTarget, NamedOutItem, OneOut, OrderDirection,
    OrderingSpec, OutItem, PivotSpec, ReductionItem, RenameSpec, RepositionSpec,
    TupleOrdinalClause, TupleOrdinalOperator,
};
