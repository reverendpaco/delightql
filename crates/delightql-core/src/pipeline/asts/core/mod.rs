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

pub use columns::{AtSign, AuthoredColumn, ContextMarker, WrittenBinder};
pub use expressions::functions::{judge_clause_order, ClauseArm, ClauseCrossing, ClauseSelection};
pub use expressions::truth::NamedProof;
pub use expressions::{
    Access, AnonRelation, AnonTable, ArgumentValue, ArrayPattern, ArrayPatternMember,
    BagCorrelation, Callable, CaseExpression, Chain, Collection, Comparison, Continuation,
    CorrPred, Correspondence, Crossing, Datum, DestructurePattern, DomainExpression, DomainHole,
    Enclyph, ErJoinStep, Existence, FactFunctionArm, FactFunctionDefinition, FactFunctionMode,
    FieldSelect, FilterOrigin, FunctionApplication, FunctorCall, Glob, Grelex,
    GroundForm, GroundMention, HeaderItem, InfixApplication, IterationPattern, JsonAccess, Lambda,
    MatchArm, MemberCorrelation, Membership, MembershipSource, MetadataBinding, MetadataGroup,
    MetadataTarget, ModeWitness, NamedReference, NestedPattern, Path, PathBinding, PathStep,
    PatternTarget, Peel, Polarity, Probe, ProbeAddressing, PureCall, QualifiedName, Record,
    RecordMember, RecordPattern, RecordPatternMember, ReductionPlan, Reference, RegexCase,
    RegexSelector, Relation, RelationalMembership, RenameSource, ScalarRelation,
    Scalarization, ScalarizedRelation, SealedCall, SearchedArm, SelectorItem, SetOperator,
    SigmaApplication, Slot, Spread, StandardApplication, Step, StructuralForm,
    StructuralStep, TabularBody, TabularRow, Transparent, TreeGroupPlan,
    TreePattern, TruthExpression, Tuple, TupleElement, ValueRow, ValueTemplate,
    ValueTemplatePart, WholeHeading, WindowSpec,
};
pub use literals::{ColumnOrdinal, ColumnRange, CompileTimeInteger, LiteralValue, NumericCategory, NumericLiteral};
pub use metadata::NamespacePath;
pub use operators::{FrameBound, JoinRoles, PipeOp, WindowFrame};
pub use phases::{Phase, Unresolved};
pub use queries::{
    AuthoredCteSubject, CfeClause, CfeDefinition, CfeFormals, ContextMode, CteAuthority,
    CteBinding, CteEffectDeclaration, CteSubjectView, DangerSpec, DangerState,
    HoDefinition, InlineDdlBody, InlineDdlSpec, LexicalHorizon, OptionSpec, OptionState, Query,
    QueryLocalBlock, QueryLocalNames, QueryLocals, SigmaDefinition, StatementBlocks,
};
pub(crate) use queries::{QueryLocalDemand, QueryLocalKind};
pub use specs::{
    DelegateSpec, GroupSpec, MetadataOut, NameTarget, NamedOutItem, OneOut, OrderDirection,
    OrderingSpec, OutItem, PivotSpec, ReductionItem, RenameSpec, RepositionSpec,
    TupleOrdinalClause, TupleOrdinalOperator,
};
