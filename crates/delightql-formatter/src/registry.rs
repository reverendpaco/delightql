// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The weld between the grammar's alphabet and the visitor.
//!
//! Every CONCRETE kind the consolidated grammar can put in a tree stands in
//! exactly one of these lists — `tests/weld.rs` enforces the partition against
//! `delightql_cst::cst::ALL`, which the grammar itself writes. A grammar change
//! that adds a kind goes red there until the formatter takes a position. There
//! is no wildcard: a new semantic form cannot arrive and be echoed unnoticed.
//!
//! Supertypes are excluded because no node ever HAS a supertype's kind; the
//! typed enums over them are how the visitor matches, and Rust makes those
//! exhaustive already.

use delightql_cst::cst::Kind;

/// Kinds a typed arm lays out: it decides breaks, indentation, or spacing that
/// the author's own whitespace does not settle.
///
/// Meeting one of these in the ECHO writer is a registry lie — the visitor lost
/// a node its arm was supposed to receive — and surfaces as a pass-through
/// naming the kind, exactly as an unplaced kind would.
pub const LAID_OUT: &[Kind] = &[
    Kind::Cfe, Kind::CommaContinuation, Kind::CompanionCellRoot,
    Kind::DefinitionFile, Kind::EffectChain, Kind::Effrelex, Kind::Group,
    Kind::HoCte, Kind::LabelCte, Kind::LetBlock, Kind::LetFreeRelex, Kind::MapCover,
    Kind::PipeContinuation, Kind::Project, Kind::QuerySequence,
    Kind::QuerySequenceHeader, Kind::QuerySequenceRoot, Kind::Relex,
    Kind::Rename, Kind::SourceFile, Kind::StandardCte,
];

/// Kinds the visitor ECHOES: their layout is the author's, so the formatter
/// reproduces the authored tokens and takes no position beyond inter-token
/// spacing. Being here is a POSITION — "this form needs no layout" — not an
/// absence of one.
pub const VERBATIM: &[Kind] = &[
    Kind::Access, Kind::AnchoredCase, Kind::AndKeyword, Kind::AnnotationUri,
    Kind::AnonBody, Kind::AnonGrelex, Kind::AnonScalarSubquery,
    Kind::ArgumentativeForm, Kind::ArgumentativeFunctor, Kind::ArgumentativeStage,
    Kind::ArgumentativeHeading, Kind::ArmCondition, Kind::ArrayPattern,
    Kind::Arrow, Kind::AsKeyword, Kind::AsNameTemplate, Kind::AscKeyword,
    Kind::BinaryConnective, Kind::BinaryOp,
    Kind::Binder, Kind::Blob, Kind::Boolean, Kind::BoundOp,
    Kind::BoundToOne, Kind::CallableParam, Kind::Callee, Kind::CaseLike,
    Kind::CatalogFunctor, Kind::CfeParams, Kind::Citation, Kind::CmpOp,
    Kind::CommaSigil, Kind::Comment, Kind::CompanionRootMarker,
    Kind::Comparison, Kind::CompositionInput, Kind::CompressedInterior,
    Kind::CrossedTruth,
    Kind::ConfigAnnotation, Kind::ConjunctionExpression, Kind::ConstantRule,
    Kind::ConstraintTruth, Kind::ContextCapture, Kind::ContextMarker,
    Kind::CorrespondingUnionContinuation, Kind::CorrespondingUnionSigil,
    Kind::DangerAnnotation, Kind::DataRow, Kind::DdlAnnotation,
    Kind::DdlContent, Kind::DebugPoint, Kind::DeclaredRelationParam,
    Kind::DefaultArm, Kind::DefaultCell, Kind::DefinitionDoc,
    Kind::DefinitionNeck, Kind::DeicticStage, Kind::DelimitedMention,
    Kind::DescKeyword, Kind::DestructureMode, Kind::DestructureRelex,
    Kind::DestructureSigil, Kind::DisjunctionExpression, Kind::Disregarded,
    Kind::DistinctMark, Kind::DocSlot, Kind::DocText, Kind::DomainActivate,
    Kind::DoublePercentSigil, Kind::Drill, Kind::EdgeContext,
    Kind::EdgeContinuation, Kind::EdgeDeclaration, Kind::EdgeSigil,
    Kind::EdgeTerm, Kind::EffectArgumentPart, Kind::EffectArgumentativeHead,
    Kind::EffectGlobHead, Kind::EffectHoCte, Kind::EffectIdentifier, Kind::EffectLabelCte,
    Kind::EffectMarker, Kind::EffectRule, Kind::EffectStandardCte,
    Kind::EffrelexArgumentativeFunctor, Kind::EffrelexInteriorFunctor,
    Kind::Embed, Kind::EmbedMapCover, Kind::EmptyEffectArguments,
    Kind::EngineName, Kind::EngineReference, Kind::ErrorAnnotation,
    Kind::Existence, Kind::ExistsAnonGrelex, Kind::ExistsAnonOpen,
    Kind::FactArm, Kind::FactBody, Kind::FactDatum,
    Kind::FactDefault, Kind::FactForm, Kind::FactFunction, Kind::FactRow,
    Kind::FieldSelect, Kind::FixpointBadge, Kind::FoRule, Kind::Frame,
    Kind::FrameCurrentRow, Kind::FrameFollowing, Kind::FrameKind,
    Kind::FramePreceding, Kind::FrameUnbounded, Kind::FunctionPipe,
    Kind::FunctionPipeOperator, Kind::FunctionPipeStep,
    Kind::FunctionRule, Kind::Glob, Kind::GlobHeading, Kind::GoalMarker,
    Kind::GroupDelegate, Kind::Guard, Kind::GuardedParam, Kind::HeadTerm,
    Kind::HeaderItem, Kind::HeaderRow, Kind::HeadingCorrelation,
    Kind::HoArgumentReference, Kind::HoFactForm, Kind::HoPart, Kind::HoRule,
    Kind::Identifier, Kind::InKeyword, Kind::InchoateFunctor,
    Kind::IndexedBinding, Kind::InducedMember, Kind::InfixOperator,
    Kind::InnerArgumentRow,
    Kind::InsertSource, Kind::Interior, Kind::InteriorContinuation,
    Kind::InteriorFunctor, Kind::Interpolation, Kind::Iteration,
    Kind::JsonAccess, Kind::JsonAccessor, Kind::Key, Kind::KeyColumn,
    Kind::KeyedBinding, Kind::KeyedMetadata, Kind::KeyedValue, Kind::Lambda, Kind::LambdaBinder,
    Kind::Landing, Kind::LeadingOuterGrelex, Kind::LiftSigil,
    Kind::LowerOrderEffrelex, Kind::MarkedTarget, Kind::MatchArm,
    Kind::Membership, Kind::Meta, Kind::MetaSigil,
    Kind::MetadataBinding, Kind::MetadataGroup, Kind::MetadataSigil,
    Kind::MinusContinuation, Kind::MinusSigil, Kind::MutationMarker,
    Kind::MutationSource, Kind::NameTemplatePlaceholder,
    Kind::NameTemplateText, Kind::NamedGroupKey, Kind::NamedOutItem,
    Kind::NamedReference, Kind::Namespace, Kind::NamespaceQual,
    Kind::Naming, Kind::NarrowingAccess, Kind::NarrowingDestructure,
    Kind::Negation, Kind::NestedPattern, Kind::NotKeyword, Kind::Null,
    Kind::Number, Kind::OfKeyword, Kind::One, Kind::OpenFunctor,
    Kind::OpenRelationParam, Kind::OpenWindowFunctor, Kind::OrKeyword,
    Kind::OrderDirection, Kind::OrderItem, Kind::Ordering, Kind::Ordinal,
    Kind::OuterAnonGrelex, Kind::OuterGrelex, Kind::OuterMarker,
    Kind::OuterPeer, Kind::ParenthesizedOperand, Kind::ParenthesizedTruth,
    Kind::Partition, Kind::Path, Kind::PathBinding, Kind::PathName,
    Kind::PercentSigil, Kind::PipeOperator, Kind::Pivot, Kind::PlainParam,
    Kind::Polarity, Kind::PositionalHeading, Kind::PositionalReference,
    Kind::PositionalSpan, Kind::PositionalUnionContinuation,
    Kind::PositionalUnionSigil, Kind::PostPipeEffrelex,
    Kind::PredicateIdentifier, Kind::PrimaryKeySigil, Kind::ProbeRow,
    Kind::ProjectOut, Kind::PureInvocation, Kind::QualifierName,
    Kind::Record, Kind::RecordPattern, Kind::ReductionSigil, Kind::Regex,
    Kind::RelationName, Kind::RelationalMembership, Kind::RenamePair,
    Kind::RenamedSlot, Kind::Reposition, Kind::RepositionPair,
    Kind::ReservedAnnotation, Kind::ReservedText, Kind::ResidualDesignator,
    Kind::RowBound, Kind::RuleParam, Kind::ScalarParam,
    Kind::ScalarParameterReference, Kind::ScalarSubquery, Kind::SearchedArm,
    Kind::SearchedCase, Kind::Selector, Kind::SelfKeyedReference,
    Kind::Separator, Kind::SigmaApplication, Kind::SigmaBody,
    Kind::SigmaRule, Kind::SignedWitness, Kind::SignedWitnessSigil,
    Kind::SingletonReduction, Kind::Skipped, Kind::SmartComment,
    Kind::SmartUnionContinuation, Kind::SmartUnionSigil, Kind::SparseFill,
    Kind::SparseMark, Kind::StageName, Kind::StandardApplication,
    Kind::StarSigil, Kind::StopPoint, Kind::StringNode, Kind::StroppedForm,
    Kind::Symbol, Kind::Template, Kind::TemplateText, Kind::TopLevelGoal,
    Kind::Transform, Kind::TransformItem, Kind::TransformNaming,
    Kind::TransitiveEdgeSigil, Kind::TripleTemplatePart,
    Kind::TripleTemplateText, Kind::Tuple,
    Kind::UniqueKeySigil, Kind::UnwrapPipeOperator, Kind::UriSegment,
    Kind::Using, Kind::ValueArgument, Kind::ValueRow,
    Kind::WindowApplication, Kind::WindowSigil, Kind::WindowSpec,
    Kind::Witness,
];

/// What the formatter does with a kind.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Policy {
    LaidOut,
    Verbatim,
    /// In neither list. Only reachable when the registry and the grammar have
    /// drifted apart, which is what the weld exists to prevent.
    Unplaced,
}

pub fn policy(kind: Kind) -> Policy {
    if VERBATIM.contains(&kind) {
        Policy::Verbatim
    } else if LAID_OUT.contains(&kind) {
        Policy::LaidOut
    } else {
        Policy::Unplaced
    }
}
