use super::{Addressed, CprSchema, Refined, Resolved, Unresolved};
use crate::lispy::ToLispy;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Debug;
use std::marker::PhantomData;

/// PhaseBox provides compile-time enforcement of schema access patterns across compilation phases.
///
/// Unlike Option<CprSchema>, PhaseBox makes invalid operations impossible at compile time:
/// - Unresolved phase can only create phantom boxes (no access to data)
/// - Resolved phase can create and read schemas
/// - Refined phase can only read schemas (no creation)
pub struct PhaseBox<T, Phase> {
    data: T,
    _phase: PhantomData<Phase>,
}

// =============================================================================
// Trait Implementations
// =============================================================================

impl<T: Clone, P> Clone for PhaseBox<T, P> {
    fn clone(&self) -> Self {
        PhaseBox {
            data: self.data.clone(),
            _phase: PhantomData,
        }
    }
}

impl<T: Debug, P> Debug for PhaseBox<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PhaseBox({:?})", self.data)
    }
}

impl<T: PartialEq, P> PartialEq for PhaseBox<T, P> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<T: Serialize, P> Serialize for PhaseBox<T, P> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.data.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>, P> Deserialize<'de> for PhaseBox<T, P> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(PhaseBox {
            data: T::deserialize(deserializer)?,
            _phase: PhantomData,
        })
    }
}

// =============================================================================
// Phase-Specific Implementations for CprSchema
// =============================================================================

// Trait for types that can create phantom data
pub trait PhantomAble {
    fn phantom_data() -> Self;
}

// Trait for types that can be put into a PhaseBox
pub trait PhaseBoxable {
    type Phase;
    fn new(self) -> PhaseBox<Self, Self::Phase>
    where
        Self: Sized;
}

// Generic phantom method for any type that implements PhantomAble
impl<D: PhantomAble, Phase> PhaseBox<D, Phase> {
    /// Creates a phantom box with dummy data.
    pub fn phantom() -> Self {
        PhaseBox {
            data: D::phantom_data(),
            _phase: PhantomData,
        }
    }
}

// Phase-change method that preserves data
impl<T, P> PhaseBox<T, P> {
    /// Changes the phase tag while preserving the contained data.
    ///
    /// For P=Q (same-phase transforms like grounding, CFE substitution), this is
    /// an identity operation preserving data exactly like AstFold did.
    /// For P≠Q (cross-phase transforms like the resolver), hooks override the
    /// result anyway, so data preservation is harmless.
    pub fn rephase<Q>(self) -> PhaseBox<T, Q> {
        PhaseBox {
            data: self.data,
            _phase: PhantomData,
        }
    }
}

// Generic new method that delegates to the trait
impl<T: PhaseBoxable> PhaseBox<T, T::Phase> {
    pub fn new(data: T) -> Self {
        data.new()
    }
}

// Implement PhantomAble for bool (used by CteBinding.is_recursive)
impl PhantomAble for bool {
    fn phantom_data() -> Self {
        false
    }
}

// Implement PhantomAble for CprSchema
impl PhantomAble for CprSchema {
    fn phantom_data() -> Self {
        CprSchema::Unresolved(vec![])
    }
}

// Implement PhantomAble for ColumnOrdinal
impl PhantomAble for super::ColumnOrdinal {
    fn phantom_data() -> Self {
        super::ColumnOrdinal {
            position: 0,
            reverse: false,
            qualifier: None,
            namespace_path: super::metadata::NamespacePath::empty(),
            alias: None,
            glob: false,
        }
    }
}

// Implement PhantomAble for ColumnRange
impl PhantomAble for super::ColumnRange {
    fn phantom_data() -> Self {
        super::ColumnRange {
            start: None,
            end: None,
            qualifier: None,
            namespace_path: super::metadata::NamespacePath::empty(),
        }
    }
}

// Implement PhantomAble for Option<LvarProvenance>
impl PhantomAble for Option<super::expressions::domain::LvarProvenance> {
    fn phantom_data() -> Self {
        None
    }
}

// Implement PhantomAble for Option<String> (used by CteRequirements.cte_name)
impl PhantomAble for Option<String> {
    fn phantom_data() -> Self {
        None
    }
}

// Implement PhantomAble for Option<SqlIdentifier> (used by canonical_name on Relation::Ground)
impl PhantomAble for Option<delightql_types::SqlIdentifier> {
    fn phantom_data() -> Self {
        None
    }
}

// Implement PhantomAble for ScopedSchema (ConsultedView doesn't exist in Unresolved, but type system needs this)
impl PhantomAble for ScopedSchema {
    fn phantom_data() -> Self {
        ScopedSchema::from_parts(
            delightql_types::SqlIdentifier::new("__phantom"),
            CprSchema::Unknown,
        )
    }
}

// Implement PhantomAble for Vec<DestructureMapping> (used by Destructure destructured_schema)
impl PhantomAble for Vec<super::expressions::pipes::DestructureMapping> {
    fn phantom_data() -> Self {
        Vec::new()
    }
}

// Implement PhantomAble for Option<BooleanExpression<Phase>> (used by SetOperation correlation)
// A phantom correlation is always None — correlation predicates are only populated during refinement.
impl<Phase> PhantomAble for Option<BooleanExpression<Phase>> {
    fn phantom_data() -> Self {
        None
    }
}

// -----------------------------------------------------------------------------
// Unresolved Phase: Can only create phantom box
// -----------------------------------------------------------------------------
impl PhaseBox<CprSchema, Unresolved> {}

// -----------------------------------------------------------------------------
// Resolved Phase: Can create and read
// -----------------------------------------------------------------------------
impl PhaseBox<CprSchema, Resolved> {
    /// Gets the schema (guaranteed to exist in Resolved phase).
    pub fn get(&self) -> &CprSchema {
        &self.data
    }

    /// Transitions to Refined phase.
    /// Consumes self to prevent accidental reuse of Resolved schemas.
    pub fn into_refined(self) -> PhaseBox<CprSchema, Refined> {
        PhaseBox {
            data: self.data,
            _phase: PhantomData,
        }
    }
}

// -----------------------------------------------------------------------------
// Refined Phase: Read-only
// -----------------------------------------------------------------------------
impl PhaseBox<CprSchema, Refined> {
    /// Gets the schema (guaranteed to exist in Refined phase).
    pub fn get(&self) -> &CprSchema {
        &self.data
    }
}

// =============================================================================
// PhaseBox for Vec<DestructureMapping> (used by Destructure destructured_schema)
// =============================================================================

use super::expressions::pipes::DestructureMapping;

// Resolved Phase: Can create and read
impl PhaseBox<Vec<DestructureMapping>, Resolved> {
    /// Create a new PhaseBox with the given mappings
    pub fn from_mappings(mappings: Vec<DestructureMapping>) -> Self {
        PhaseBox {
            data: mappings,
            _phase: PhantomData,
        }
    }

    /// Gets the destructured key mappings.
    pub fn data(&self) -> &Vec<DestructureMapping> {
        &self.data
    }

    /// Transitions to Refined phase.
    pub fn into_refined(self) -> PhaseBox<Vec<DestructureMapping>, Refined> {
        PhaseBox {
            data: self.data,
            _phase: PhantomData,
        }
    }
}

// Refined Phase: Read-only
impl PhaseBox<Vec<DestructureMapping>, Refined> {
    /// Gets the destructured key mappings.
    pub fn data(&self) -> &Vec<DestructureMapping> {
        &self.data
    }
}

// Phase conversion for Vec<DestructureMapping>
impl From<PhaseBox<Vec<DestructureMapping>, Resolved>>
    for PhaseBox<Vec<DestructureMapping>, Refined>
{
    fn from(resolved: PhaseBox<Vec<DestructureMapping>, Resolved>) -> Self {
        PhaseBox {
            data: resolved.data,
            _phase: PhantomData,
        }
    }
}

// -----------------------------------------------------------------------------
// Correlation handling for SetOperation (phase-specific)
// -----------------------------------------------------------------------------

use super::BooleanExpression;

// Unresolved phase: Can only create empty correlation
impl<Phase> PhaseBox<Option<BooleanExpression<Phase>>, Unresolved> {
    pub fn no_correlation() -> Self {
        PhaseBox {
            data: None,
            _phase: PhantomData,
        }
    }
}

// Resolved phase: Pass through only
impl PhaseBox<Option<BooleanExpression<Resolved>>, Resolved> {
    pub fn pass_through_correlation(
        _from: PhaseBox<Option<BooleanExpression<Unresolved>>, Unresolved>,
    ) -> Self {
        PhaseBox {
            data: None,
            _phase: PhantomData,
        }
    }
}

// Refined phase: Can actually set correlation
impl PhaseBox<Option<BooleanExpression<Refined>>, Refined> {
    pub fn with_correlation(expr: Option<BooleanExpression<Refined>>) -> Self {
        PhaseBox {
            data: expr,
            _phase: PhantomData,
        }
    }

    pub fn has_correlation(&self) -> bool {
        self.data.is_some()
    }

    pub fn get_correlation(&self) -> &Option<BooleanExpression<Refined>> {
        &self.data
    }
}

// Phase conversion for correlation PhaseBox
impl From<PhaseBox<Option<BooleanExpression<Unresolved>>, Unresolved>>
    for PhaseBox<Option<BooleanExpression<Resolved>>, Resolved>
{
    fn from(_: PhaseBox<Option<BooleanExpression<Unresolved>>, Unresolved>) -> Self {
        PhaseBox {
            data: None,
            _phase: PhantomData,
        }
    }
}

impl From<PhaseBox<Option<BooleanExpression<Resolved>>, Resolved>>
    for PhaseBox<Option<BooleanExpression<Refined>>, Refined>
{
    fn from(resolved: PhaseBox<Option<BooleanExpression<Resolved>>, Resolved>) -> Self {
        PhaseBox {
            data: resolved.data.map(Into::into),
            _phase: PhantomData,
        }
    }
}

// =============================================================================
// ToLispy Implementation
// =============================================================================

impl<T: ToLispy, P> ToLispy for PhaseBox<T, P> {
    fn to_lispy(&self) -> String {
        self.data.to_lispy()
    }
}

// =============================================================================
// PhaseBoxable Implementations
// =============================================================================

// CprSchema belongs in Resolved phase when created by resolver
impl PhaseBoxable for CprSchema {
    type Phase = Resolved;

    fn new(self) -> PhaseBox<Self, Self::Phase> {
        PhaseBox {
            data: self,
            _phase: PhantomData,
        }
    }
}

// ScopedSchema belongs in Resolved phase when created by resolver
impl PhaseBoxable for ScopedSchema {
    type Phase = Resolved;

    fn new(self) -> PhaseBox<Self, Self::Phase> {
        PhaseBox {
            data: self,
            _phase: PhantomData,
        }
    }
}

// ColumnOrdinal belongs in Unresolved phase when created by builder
impl PhaseBoxable for ColumnOrdinal {
    type Phase = Unresolved;

    fn new(self) -> PhaseBox<Self, Self::Phase> {
        PhaseBox {
            data: self,
            _phase: PhantomData,
        }
    }
}

// ColumnRange belongs in Unresolved phase when created by builder
impl PhaseBoxable for ColumnRange {
    type Phase = Unresolved;

    fn new(self) -> PhaseBox<Self, Self::Phase> {
        PhaseBox {
            data: self,
            _phase: PhantomData,
        }
    }
}

// bool belongs in Addressed phase (populated by addresser for is_recursive)
impl PhaseBoxable for bool {
    type Phase = Addressed;

    fn new(self) -> PhaseBox<Self, Self::Phase> {
        PhaseBox {
            data: self,
            _phase: PhantomData,
        }
    }
}

// Option<LvarProvenance> belongs in Refined phase (only populated during refinement)
impl PhaseBoxable for Option<super::expressions::domain::LvarProvenance> {
    type Phase = Refined;

    fn new(self) -> PhaseBox<Self, Self::Phase> {
        PhaseBox {
            data: self,
            _phase: PhantomData,
        }
    }
}

// Add get() method for Option<LvarProvenance> in Refined phase
impl PhaseBox<Option<super::expressions::domain::LvarProvenance>, Refined> {
    pub fn get(&self) -> &Option<super::expressions::domain::LvarProvenance> {
        &self.data
    }
}

// =============================================================================
// PhaseBox for Option<SqlIdentifier> (used by Relation::Ground.canonical_name)
// =============================================================================

// Resolved Phase: Can create and read canonical name
impl PhaseBox<Option<delightql_types::SqlIdentifier>, Resolved> {
    /// Gets the canonical name if present
    pub fn get(&self) -> Option<&delightql_types::SqlIdentifier> {
        self.data.as_ref()
    }

    /// Transitions to Refined phase.
    pub fn into_refined(self) -> PhaseBox<Option<delightql_types::SqlIdentifier>, Refined> {
        PhaseBox {
            data: self.data,
            _phase: PhantomData,
        }
    }
}

// Refined Phase: Read-only access to canonical name
impl PhaseBox<Option<delightql_types::SqlIdentifier>, Refined> {
    /// Gets the canonical name if present
    pub fn get(&self) -> Option<&delightql_types::SqlIdentifier> {
        self.data.as_ref()
    }
}

// PhaseBoxable: Option<SqlIdentifier> belongs in Resolved phase
impl PhaseBoxable for Option<delightql_types::SqlIdentifier> {
    type Phase = Resolved;

    fn new(self) -> PhaseBox<Self, Self::Phase> {
        PhaseBox {
            data: self,
            _phase: PhantomData,
        }
    }
}

// Phase conversion for Option<SqlIdentifier>
impl From<PhaseBox<Option<delightql_types::SqlIdentifier>, Resolved>>
    for PhaseBox<Option<delightql_types::SqlIdentifier>, Refined>
{
    fn from(resolved: PhaseBox<Option<delightql_types::SqlIdentifier>, Resolved>) -> Self {
        resolved.into_refined()
    }
}

// =============================================================================
// PhaseBox for Option<String>
// =============================================================================

// Addressed Phase: Can create and read (used by CteRequirements.cte_name)
impl PhaseBox<Option<String>, Addressed> {
    pub fn from_cte_name(name: Option<String>) -> Self {
        PhaseBox {
            data: name,
            _phase: PhantomData,
        }
    }

    pub fn get(&self) -> &Option<String> {
        &self.data
    }
}

// Resolved Phase: Can create and read backend schema
impl PhaseBox<Option<String>, Resolved> {
    /// Create a new PhaseBox with optional backend schema name
    pub fn from_optional_schema(schema: Option<String>) -> Self {
        PhaseBox {
            data: schema,
            _phase: PhantomData,
        }
    }

    /// Gets the backend schema if present
    pub fn get(&self) -> &Option<String> {
        &self.data
    }
}

// Implement Default for deserialization support
impl Default for PhaseBox<Option<super::expressions::domain::LvarProvenance>, Refined> {
    fn default() -> Self {
        PhaseBox {
            data: None,
            _phase: PhantomData,
        }
    }
}

// =============================================================================
// Conversion Support for Phase Transitions
// =============================================================================

/// Support for converting PhaseBox during phase transitions.
/// This is used by the From implementations for AST nodes.
impl From<PhaseBox<CprSchema, Resolved>> for PhaseBox<CprSchema, Refined> {
    fn from(resolved: PhaseBox<CprSchema, Resolved>) -> PhaseBox<CprSchema, Refined> {
        resolved.into_refined()
    }
}

// =============================================================================
// PhaseBox for ScopedSchema (ConsultedView alias+schema binding)
// =============================================================================

use super::ScopedSchema;

impl PhaseBox<ScopedSchema, Resolved> {
    pub fn get(&self) -> &ScopedSchema {
        &self.data
    }

    pub fn into_refined(self) -> PhaseBox<ScopedSchema, Refined> {
        PhaseBox {
            data: self.data,
            _phase: PhantomData,
        }
    }
}

impl PhaseBox<ScopedSchema, Refined> {
    pub fn get(&self) -> &ScopedSchema {
        &self.data
    }
}

impl From<PhaseBox<ScopedSchema, Resolved>> for PhaseBox<ScopedSchema, Refined> {
    fn from(resolved: PhaseBox<ScopedSchema, Resolved>) -> PhaseBox<ScopedSchema, Refined> {
        resolved.into_refined()
    }
}

// =============================================================================
// PhaseBox for ColumnOrdinal
// =============================================================================

use super::ColumnOrdinal;

// Unresolved Phase: Builder creates, Resolver reads
impl PhaseBox<ColumnOrdinal, Unresolved> {
    pub fn get(&self) -> &ColumnOrdinal {
        &self.data
    }
    pub fn get_mut(&mut self) -> &mut ColumnOrdinal {
        &mut self.data
    }
}

// =============================================================================
// PhaseBox for ColumnRange
// =============================================================================

use super::ColumnRange;

// Unresolved Phase: Builder creates, Resolver reads
impl PhaseBox<ColumnRange, Unresolved> {
    pub fn get(&self) -> &ColumnRange {
        &self.data
    }
}

// =============================================================================
// Addressed Phase — identity pass-through from Refined
// =============================================================================

// CprSchema: Refined → Addressed
impl PhaseBox<CprSchema, Refined> {
    pub fn into_addressed(self) -> PhaseBox<CprSchema, Addressed> {
        PhaseBox {
            data: self.data,
            _phase: PhantomData,
        }
    }
}

impl PhaseBox<CprSchema, Addressed> {
    pub fn get(&self) -> &CprSchema {
        &self.data
    }
}

// bool: Addressed phase — read access (copy, not ref)
impl PhaseBox<bool, Addressed> {
    pub fn get(&self) -> bool {
        self.data
    }
}

impl From<PhaseBox<CprSchema, Refined>> for PhaseBox<CprSchema, Addressed> {
    fn from(refined: PhaseBox<CprSchema, Refined>) -> Self {
        refined.into_addressed()
    }
}

// ScopedSchema: Refined → Addressed
impl PhaseBox<ScopedSchema, Refined> {
    pub fn into_addressed(self) -> PhaseBox<ScopedSchema, Addressed> {
        PhaseBox {
            data: self.data,
            _phase: PhantomData,
        }
    }
}

impl PhaseBox<ScopedSchema, Addressed> {
    pub fn get(&self) -> &ScopedSchema {
        &self.data
    }
}

impl From<PhaseBox<ScopedSchema, Refined>> for PhaseBox<ScopedSchema, Addressed> {
    fn from(refined: PhaseBox<ScopedSchema, Refined>) -> Self {
        refined.into_addressed()
    }
}

// Vec<DestructureMapping>: Refined → Addressed
impl PhaseBox<Vec<DestructureMapping>, Refined> {
    pub fn into_addressed(self) -> PhaseBox<Vec<DestructureMapping>, Addressed> {
        PhaseBox {
            data: self.data,
            _phase: PhantomData,
        }
    }
}

impl PhaseBox<Vec<DestructureMapping>, Addressed> {
    pub fn data(&self) -> &Vec<DestructureMapping> {
        &self.data
    }
}

impl From<PhaseBox<Vec<DestructureMapping>, Refined>>
    for PhaseBox<Vec<DestructureMapping>, Addressed>
{
    fn from(refined: PhaseBox<Vec<DestructureMapping>, Refined>) -> Self {
        refined.into_addressed()
    }
}

// Correlation: Refined → Addressed
impl PhaseBox<Option<BooleanExpression<Addressed>>, Addressed> {
    pub fn with_correlation(expr: Option<BooleanExpression<Addressed>>) -> Self {
        PhaseBox {
            data: expr,
            _phase: PhantomData,
        }
    }

    pub fn has_correlation(&self) -> bool {
        self.data.is_some()
    }

    pub fn get_correlation(&self) -> &Option<BooleanExpression<Addressed>> {
        &self.data
    }
}

impl From<PhaseBox<Option<BooleanExpression<Refined>>, Refined>>
    for PhaseBox<Option<BooleanExpression<Addressed>>, Addressed>
{
    fn from(refined: PhaseBox<Option<BooleanExpression<Refined>>, Refined>) -> Self {
        PhaseBox {
            data: refined.data.map(Into::into),
            _phase: PhantomData,
        }
    }
}

// Option<SqlIdentifier>: Refined → Addressed
impl PhaseBox<Option<delightql_types::SqlIdentifier>, Refined> {
    pub fn into_addressed(self) -> PhaseBox<Option<delightql_types::SqlIdentifier>, Addressed> {
        PhaseBox {
            data: self.data,
            _phase: PhantomData,
        }
    }
}

impl PhaseBox<Option<delightql_types::SqlIdentifier>, Addressed> {
    pub fn get(&self) -> Option<&delightql_types::SqlIdentifier> {
        self.data.as_ref()
    }
}

impl From<PhaseBox<Option<delightql_types::SqlIdentifier>, Refined>>
    for PhaseBox<Option<delightql_types::SqlIdentifier>, Addressed>
{
    fn from(refined: PhaseBox<Option<delightql_types::SqlIdentifier>, Refined>) -> Self {
        refined.into_addressed()
    }
}

// Option<LvarProvenance>: Refined → Addressed
impl PhaseBox<Option<super::expressions::domain::LvarProvenance>, Addressed> {
    pub fn get(&self) -> &Option<super::expressions::domain::LvarProvenance> {
        &self.data
    }
}

impl From<PhaseBox<Option<super::expressions::domain::LvarProvenance>, Refined>>
    for PhaseBox<Option<super::expressions::domain::LvarProvenance>, Addressed>
{
    fn from(refined: PhaseBox<Option<super::expressions::domain::LvarProvenance>, Refined>) -> Self {
        PhaseBox {
            data: refined.data,
            _phase: PhantomData,
        }
    }
}

impl Default for PhaseBox<Option<super::expressions::domain::LvarProvenance>, Addressed> {
    fn default() -> Self {
        PhaseBox {
            data: None,
            _phase: PhantomData,
        }
    }
}
