// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The registry: the only producer of handles, and the only holder of
//! characters.
//!
//! One per compilation. No `Default`, no `static`, no `lazy_static`, no
//! thread-local — an emitted name must not depend on how many queries this
//! process compiled earlier, and the way to guarantee that is to have no
//! counter that survives a compilation.
//!
//! Every record field is private to this module. The ask methods return
//! handles and copied facts; not one of them returns characters. Characters
//! leave only through [`IdentSink`](super::sink::IdentSink).

use std::cell::RefCell;
use std::collections::HashMap;

use super::id::{CallableCategory, CallableId, ColId, EntityId, FnId, ScopeId, Spelling, Sym};
use super::origin::{
    Addressing, ColumnOrigin, FnOrigin, FunctionSpellingError, Hint, Intrinsic, Republish,
    ScopeOrigin, ValueFacts,
};
use super::sink::IdentSink;

/// What the registry can say about a scope's heading.
///
/// `Opaque` means the target did not publish enough metadata to enumerate
/// the dimensions. It is NOT a zero-column heading: "I cannot say what this
/// publishes" and "this publishes nothing" are different claims, and a
/// reader that receives an empty list for the first one goes on to report a
/// dimension as absent from an enumeration that was never made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HeadingKnowledge {
    Known(Candidates<ColId>),
    Opaque,
}

impl HeadingKnowledge {
    /// The ordered dimensions, for a reader that needs structural column
    /// identities. An opaque relation refuses here, once, and says where to
    /// declare them.
    pub fn structural(self) -> crate::error::Result<Candidates<ColId>> {
        match self {
            HeadingKnowledge::Known(heading) => Ok(heading),
            HeadingKnowledge::Opaque => {
                Err(crate::error::DelightQLError::validation_error_categorized(
                    crate::uri_registry::subcat::RESOLUTION_SCHEMA,
                    "this relation's heading is not published by the target, so its \
                     dimensions cannot be named here",
                    "declare the dimensions at the mention — `f(...)(a, b)` names one \
                     slot per dimension of the full width",
                ))
            }
        }
    }

    pub fn is_opaque(&self) -> bool {
        matches!(self, HeadingKnowledge::Opaque)
    }

    /// The dimensions actually seen, for a reader that GATHERS rather than
    /// concludes.
    ///
    /// An opaque heading yields none, which is why this returns a plain
    /// vector and not `Candidates`: nothing here is an exhaustive
    /// enumeration, so nothing built from it may be read as one. A caller
    /// that goes on to say a name is absent, or that one relation is the
    /// sole owner of something, is drawing a conclusion and wants
    /// `structural` instead.
    pub fn columns_seen(self) -> Vec<ColId> {
        match self {
            HeadingKnowledge::Known(heading) => heading.into_vec(),
            HeadingKnowledge::Opaque => Vec::new(),
        }
    }
}

/// An exhaustively enumerated set of possible answers.
///
/// EXHAUSTIVELY: every conclusion a caller draws from one of these —
/// absent, unique, how many, who owns it — is a claim about the whole
/// enumeration, and the type is the promise that the enumeration was whole.
/// A relation whose dimensions the target never published cannot produce
/// one; `HeadingKnowledge` is where that case lives, and it does not hand
/// out a set at all.
///
/// The wrapper keeps collection operations available while making the
/// authority's choice point explicit: code may inspect or carry candidates,
/// but it must not silently choose the first one.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Candidates<T> {
    values: Vec<T>,
}

impl<T> Candidates<T> {
    pub(crate) fn from_vec(values: Vec<T>) -> Self {
        Candidates { values }
    }

    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.values.iter()
    }

    /// Enter the producer's deterministic order only at an explicit choice
    /// boundary. Enumeration and cardinality remain available without
    /// granting callers slice or index syntax.
    pub fn in_order(&self) -> std::slice::Iter<'_, T> {
        self.values.iter()
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn to_vec(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.values.clone()
    }

    pub(crate) fn into_vec(self) -> Vec<T> {
        self.values
    }
}

impl<T> IntoIterator for Candidates<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl<T> FromIterator<T> for Candidates<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Candidates::from_vec(iter.into_iter().collect())
    }
}

/// The spelling that POINTS at the one visible anonymous pipe stage.
///
/// Deixis, not a name: it selects by there being exactly one thing to point
/// at, and a relation reached this way answers to nothing else a user could
/// write. It is a tier below every real name, so an authored `_` shadows it.
const ANONYMOUS_DEIXIS: &str = "_";

/// One authored spelling and the canonical identity it folds to.
struct SpellingRecord {
    text: String,
    stropped: bool,
    canon: Sym,
}

struct ScopeRecord {
    origin: ScopeOrigin,
    /// A compiler-only prefix for baptism. Authored scopes carry no prefix.
    emission_prefix: Option<&'static str>,
    /// A compiler-owned emitted base that may be runtime-derived. Authored
    /// scopes carry neither this nor `emission_prefix`.
    emission_name: Option<Spelling>,
    /// Lexical enclosure, for correlation reachability.
    parent: Option<ScopeId>,
    /// The authored spelling a qualified reference may use to reach this
    /// scope. `None` means the scope is unreachable by qualifier — which is
    /// the correct answer for every compiler wrap.
    answers_to: Option<Spelling>,
    /// The published heading, ordered. Meaningful only when the heading is
    /// enumerable; see `opaque`.
    cols: Vec<ColId>,
    /// The target published no metadata to enumerate this scope's
    /// dimensions. `cols` says nothing here and must not be read as a
    /// heading.
    opaque: bool,
    /// Source occurrences omitted from the heading by USING, but still
    /// reachable through the live relation arm that owns them.
    qualified_carriers: Vec<ColId>,
    /// The marked relation occurrences this one stands on — the `!!`
    /// evidence, carried by the relation rather than looked for in the
    /// syntax that produced it.
    ///
    /// Each entry is the scope the mark was written on and the relation
    /// spelling it was written with — a relation of any kind, because `!!`
    /// is written on an access and a temp table or a name is as writable as
    /// a catalog table. Naming a relation, aliasing it, publishing it as
    /// a CTE and joining it all mint scopes FROM this one, so the evidence
    /// arrives wherever the relation does; a join carries both arms', which
    /// is what makes "two relations are marked" a countable fact instead of
    /// a search that can come up short.
    mutation_marks: Vec<(ScopeId, Spelling)>,
    /// This scope is emitted as a relation the statement cannot alias, so
    /// its name is the relation's and is not available for arbitration.
    ///
    /// A mutation's target is the case: the statement writes the physical
    /// relation name and every reference to its columns must render the
    /// same characters, so a rename would leave the two disagreeing.
    fixed_relation: bool,
    /// The rows this scope offers were chosen by POSITION — an ordering plus
    /// a count — rather than by a property every row satisfies.
    ///
    /// Stamped once, where the bound is established, and inherited by every
    /// scope minted from it. A reader asks the scope; it never re-derives the
    /// answer by looking for a LIMIT in emitted SQL, which is a search whose
    /// failure is silent and whose false answer is "unbounded".
    row_bound: bool,
    /// A positional reference reached one of this scope's columns. For an
    /// inchoate occurrence that reach is ACTIVATION: position reaches what
    /// names cannot, so the occurrence yields its rows.
    ordinal_reached: bool,
    /// An unaccessed inchoate occurrence: nothing activated it, so it
    /// lowers to zero rows under its opaque displayed heading.
    annihilated: bool,
}

struct ColRecord {
    /// THE qualifier fact, in one copy.
    scope: ScopeId,
    origin: ColumnOrigin,
    /// The spelling the user should see, if it has one. `None` means the
    /// column is compiler-anonymous and baptism will name it.
    published: Option<Spelling>,
    addressing: Addressing,
    facts: ValueFacts,
}

struct EntityRecord {
    canonical: Spelling,
    backend_schema: Option<Spelling>,
}

struct FunctionRecord {
    origin: FnOrigin,
    authored: Option<Spelling>,
    namespace: Vec<Spelling>,
    category: Option<CallableCategory>,
}

struct Inner {
    canon_index: HashMap<Vec<u8>, Sym>,
    canon_text: Vec<Vec<u8>>,
    spellings: Vec<SpellingRecord>,
    scopes: Vec<ScopeRecord>,
    cols: Vec<ColRecord>,
    entities: Vec<EntityRecord>,
    functions: Vec<FunctionRecord>,
    reserved: Vec<Sym>,
}

/// A reference as written: an optional qualifier and a name, both already
/// canonical. There is no `&str` here — by the time a reference reaches the
/// addressing authority it has been interned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reference {
    pub qualifier: Option<Sym>,
    pub name: Sym,
}

/// What is visible at ONE lexical position.
///
/// Visibility belongs to a position, not to a relation: the same relation
/// referenced from a join condition and from inside a correlated subquery
/// does not see the same set of other relations. Attaching a visible-set to
/// the relation itself is how that becomes a defect.
#[derive(Clone, Debug, Default)]
pub struct ScopeEnv {
    visible: Vec<ScopeId>,
    candidates: Option<Vec<ColId>>,
}

impl ScopeEnv {
    /// Build the environment for one lexical position.
    pub fn at(visible: Vec<ScopeId>) -> Self {
        let mut unique = Vec::with_capacity(visible.len());
        for scope in visible {
            if !unique.contains(&scope) {
                unique.push(scope);
            }
        }
        ScopeEnv {
            visible: unique,
            candidates: None,
        }
    }

    /// Build an environment whose candidate occurrences have already been
    /// enumerated exhaustively by the caller.
    pub(crate) fn among(visible: Vec<ScopeId>, candidates: Vec<ColId>) -> Self {
        let mut env = Self::at(visible);
        let mut unique = Vec::with_capacity(candidates.len());
        for column in candidates {
            if !unique.contains(&column) {
                unique.push(column);
            }
        }
        env.candidates = Some(unique);
        env
    }

    pub fn visible(&self) -> &[ScopeId] {
        &self.visible
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AddressError {
    /// Nothing at this position answers to the reference.
    NotFound,
    /// The candidates were not all of them: some relation in view publishes
    /// dimensions the target never described. Nothing here can be called
    /// absent, unique, or ambiguous.
    Incomplete,
    /// More than one column answers, and nothing breaks the tie.
    Ambiguous,
    /// A qualifier was written, and no visible scope answers to it.
    NoSuchScope,
    /// `_` was written where no unnamed pipe stage is in view. Distinct from
    /// [`AddressError::NoSuchScope`] because `_` is deixis rather than a
    /// name: nothing was misspelled, there is simply nothing to point at.
    NoUnnamedPipe,
    /// `_` was written with more than one unnamed pipe stage in view. One
    /// spelling cannot stand for two relations, and the writer who meant a
    /// particular one has no way to say so — until one is named with `as`.
    TwoUnnamedPipes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorrespondenceError {
    /// More than one occurrence corresponds to the requested output slot.
    Ambiguous,
    /// An arm's dimensions cannot be enumerated, so there is nothing to
    /// correspond BY. Pairing by name needs both names.
    Opaque,
}

pub struct Registry {
    inner: RefCell<Inner>,
    /// The compiler limits this compilation runs under: SHARED with the
    /// compilation executing when this arena was minted, or armed from process
    /// policy when none was.
    ///
    /// They live HERE because this object IS one compilation: a top-level
    /// compilation mints a registry, and every nested piece of work it
    /// causes — the rebuilder's re-entry, consulted views, CTEs, inner
    /// relations, assertions, compiler-built relations — is handed this
    /// same one. Budgets threaded separately would be correct only as long
    /// as every one of those call sites remembered to pass them along, and
    /// the one that forgot would silently grant its recursion a fresh
    /// allowance. They have nothing to do with identity; they are here
    /// because "the thing one compilation owns and shares" already exists
    /// and is this.
    ///
    /// They are also what the catalog publishes. A publisher that re-read
    /// process policy instead would report a number this compilation is not
    /// bounded by whenever a host moved the setting after the mint.
    limits: std::rc::Rc<crate::compiler_limits::ArmedLimits>,
}

impl Candidates<ColId> {
    /// Resolve an exhaustively enumerated set at the registry's one choice
    /// point. Chain-related republications are one occurrence; independent
    /// occurrences remain rivals and refuse as ambiguous.
    pub(crate) fn settle(self, registry: &Registry) -> Result<ColId, AddressError> {
        let mut rivals: Vec<ColId> = Vec::new();
        for hit in self.values {
            if !rivals.contains(&hit) {
                rivals.push(hit);
            }
        }
        let offered = rivals.clone();
        rivals.retain(|candidate| {
            !offered
                .iter()
                .any(|other| other != candidate && registry.republishes(*candidate, *other))
        });
        match rivals.as_slice() {
            [] => Err(AddressError::NotFound),
            [only] => Ok(*only),
            _ => Err(AddressError::Ambiguous),
        }
    }
}

/// The bytes a spelling is compared by. One function, so that reading a name
/// and interning one cannot disagree about what the identifier law folds.
fn canon_bytes_of(text: &str, stropped: bool) -> Vec<u8> {
    if stropped {
        text.bytes().collect()
    } else {
        text.bytes().map(|b| b.to_ascii_lowercase()).collect()
    }
}

impl Registry {
    /// Reserve the supplied catalog names before anything is minted.
    ///
    /// A caller that passes no reservations does not receive catalog
    /// collision protection from this constructor.
    pub fn new(catalog_reserved: &[&str]) -> Self {
        let reg = Registry {
            inner: RefCell::new(Inner {
                canon_index: HashMap::new(),
                canon_text: Vec::new(),
                spellings: Vec::new(),
                scopes: Vec::new(),
                cols: Vec::new(),
                entities: Vec::new(),
                functions: Vec::new(),
                reserved: Vec::new(),
            }),
            limits: crate::compiler_limits::ArmedLimits::in_force(),
        };
        for name in catalog_reserved {
            let s = reg.intern(name, false);
            let sym = reg.canonical(s);
            reg.inner.borrow_mut().reserved.push(sym);
        }
        reg
    }

    /// The compiler limits this compilation armed with.
    pub fn limits(&self) -> &crate::compiler_limits::ArmedLimits {
        &self.limits
    }

    /// The same, as the shareable object. Nested compiler work is handed THIS
    /// rather than a fresh pair, so the depth it is judged against and the
    /// frames it spends are both the causing compilation's.
    pub fn limits_shared(&self) -> std::rc::Rc<crate::compiler_limits::ArmedLimits> {
        std::rc::Rc::clone(&self.limits)
    }

    /// This compilation's refinement allowance.
    pub fn refinement(&self) -> &crate::refinement_budget::RefinementBudget {
        self.limits.refinement()
    }

    // ---- mint: the only producers of handles ---------------------------

    /// Record an authored spelling and return a handle to it.
    ///
    /// Two spellings that compare equal under the identifier law share a
    /// [`Sym`] but remain distinct `Spelling`s, so the characters someone
    /// typed survive alongside the identity they mean.
    pub fn intern(&self, text: &str, stropped: bool) -> Spelling {
        let canon_bytes = canon_bytes_of(text, stropped);
        let mut inner = self.inner.borrow_mut();
        let canon = match inner.canon_index.get(&canon_bytes) {
            Some(s) => *s,
            None => {
                let s = Sym(inner.canon_text.len() as u32);
                inner.canon_text.push(canon_bytes.clone());
                inner.canon_index.insert(canon_bytes, s);
                s
            }
        };
        let id = Spelling(inner.spellings.len() as u32);
        inner.spellings.push(SpellingRecord {
            text: text.to_string(),
            stropped,
            canon,
        });
        id
    }

    pub fn canonical(&self, s: Spelling) -> Sym {
        self.inner.borrow().spellings[s.0 as usize].canon
    }

    /// The name `text` means, if the registry already holds it — and nothing
    /// added if it does not.
    ///
    /// `intern` appends a `Spelling` on every call, so asking a question
    /// through it changes what is being asked about. A caller that only reads
    /// — a diagnostic, an audit, a comparison — asks here, and a name no
    /// statement has used answers `None` rather than becoming used by the act
    /// of asking.
    pub fn known_sym(&self, text: &str, stropped: bool) -> Option<Sym> {
        self.inner
            .borrow()
            .canon_index
            .get(&canon_bytes_of(text, stropped))
            .copied()
    }

    pub fn mint_entity(&self, name: Spelling) -> EntityId {
        let mut inner = self.inner.borrow_mut();
        let id = EntityId(inner.entities.len() as u32);
        inner.entities.push(EntityRecord {
            canonical: name,
            backend_schema: None,
        });
        id
    }

    /// Attach the physical spelling learned from catalog resolution.
    ///
    /// The characters remain registry-private. Lowering carries the
    /// `EntityId`; only a sealed output sink may spell it.
    pub fn bind_entity_physical(
        &self,
        entity: EntityId,
        canonical: Option<Spelling>,
        backend_schema: Option<Spelling>,
    ) {
        let mut inner = self.inner.borrow_mut();
        let record = &mut inner.entities[entity.0 as usize];
        if let Some(canonical) = canonical {
            record.canonical = canonical;
        }
        record.backend_schema = backend_schema;
    }

    pub fn mint_function(&self, name: Spelling, namespace: Vec<Spelling>) -> FnId {
        let mut inner = self.inner.borrow_mut();
        let id = CallableId(inner.functions.len() as u32);
        let origin = FnOrigin::User(inner.spellings[name.0 as usize].canon);
        inner.functions.push(FunctionRecord {
            origin,
            authored: Some(name),
            namespace,
            category: None,
        });
        id
    }

    pub fn mint_intrinsic(&self, intrinsic: Intrinsic) -> FnId {
        let mut inner = self.inner.borrow_mut();
        let id = CallableId(inner.functions.len() as u32);
        inner.functions.push(FunctionRecord {
            origin: FnOrigin::Intrinsic(intrinsic),
            authored: None,
            namespace: Vec::new(),
            category: None,
        });
        id
    }

    /// Mint the canonical callable record, including its ruled category.
    pub fn mint_callable(
        &self,
        name: Spelling,
        namespace: Vec<Spelling>,
        category: CallableCategory,
    ) -> CallableId {
        let mut inner = self.inner.borrow_mut();
        let id = CallableId(inner.functions.len() as u32);
        let origin = FnOrigin::User(inner.spellings[name.0 as usize].canon);
        inner.functions.push(FunctionRecord {
            origin,
            authored: Some(name),
            namespace,
            category: Some(category),
        });
        id
    }

    pub fn function_origin(&self, function: FnId) -> FnOrigin {
        self.inner.borrow().functions[function.0 as usize].origin
    }

    /// Return the category only for a canonical callable record. Compatibility
    /// records minted by the pre-category callers intentionally return None.
    pub fn callable_category(&self, callable: CallableId) -> Option<CallableCategory> {
        self.inner.borrow().functions[callable.0 as usize].category
    }

    /// Mint a scope occurrence. `hint` is what baptism starts from; it is
    /// never the emitted name, because the emitted name does not exist yet.
    pub fn mint_scope(&self, origin: ScopeOrigin, hint: Hint, parent: Option<ScopeId>) -> ScopeId {
        let derived_parent = self.derived_parent(&origin);
        if let (Some(supplied), Some(derived)) = (parent, derived_parent) {
            assert_eq!(
                supplied, derived,
                "a derived scope's parent must agree with its origin"
            );
        }
        let parent = derived_parent.or(parent);
        let (answers_to, emission_prefix, emission_name) = match hint {
            Hint::User(sp) => (Some(sp), None, None),
            Hint::Prefix(prefix) => (None, Some(prefix), None),
            Hint::Exact(sp) => (None, None, Some(sp)),
            Hint::None => (None, None, None),
        };
        // Read before the mint borrows, because the answer is the origin's
        // inputs and those are scopes that already exist.
        let row_bound = self.origin_is_row_bounded(&origin);
        let mutation_marks = self.origin_mutation_marks(&origin);
        let mut inner = self.inner.borrow_mut();
        let id = ScopeId(inner.scopes.len() as u32);
        inner.scopes.push(ScopeRecord {
            origin,
            emission_prefix,
            emission_name,
            parent,
            answers_to,
            cols: Vec::new(),
            opaque: false,
            qualified_carriers: Vec::new(),
            mutation_marks,
            fixed_relation: false,
            row_bound,
            ordinal_reached: false,
            annihilated: false,
        });
        id
    }

    /// Record that this relation occurrence was written `!!` — the evidence
    /// that its rows are the ones a mutation is licensed to change.
    ///
    /// Said once, where the marked access resolves. Every relation built on
    /// this one afterwards carries it, so no later reader has to reconstruct
    /// the mark by walking the syntax back to a ground name — a walk that
    /// came up empty the moment the relation was named, aliased, or joined.
    ///
    /// Idempotent per occurrence: a relation resolved twice is still one
    /// marked relation, and re-resolution must not manufacture a second.
    pub fn mark_mutation_target(&self, scope: ScopeId, relation: Spelling) {
        let mut inner = self.inner.borrow_mut();
        let marks = &mut inner.scopes[scope.0 as usize].mutation_marks;
        if !marks.iter().any(|(marked, _)| *marked == scope) {
            marks.push((scope, relation));
        }
    }

    /// A positional reference reached this scope. For an inchoate
    /// occurrence this is ACTIVATION.
    pub fn note_ordinal_reach(&self, scope: ScopeId) {
        self.inner.borrow_mut().scopes[scope.0 as usize].ordinal_reached = true;
    }

    pub fn ordinal_reached(&self, scope: ScopeId) -> bool {
        self.inner.borrow().scopes[scope.0 as usize].ordinal_reached
    }

    /// Mark an unaccessed inchoate occurrence: the lowering answers it with
    /// zero rows under the displayed heading its mints spell.
    pub fn note_annihilated(&self, scope: ScopeId) {
        self.inner.borrow_mut().scopes[scope.0 as usize].annihilated = true;
    }

    pub fn is_annihilated(&self, scope: ScopeId) -> bool {
        self.inner.borrow().scopes[scope.0 as usize].annihilated
    }

    /// Take a column's published name away: a latent dimension displays a
    /// mint, and baptism names compiler-anonymous columns.
    pub fn depublish_column(&self, column: ColId) {
        self.inner.borrow_mut().cols[column.0 as usize].published = None;
    }

    /// The marked relation occurrences this relation stands on.
    pub fn mutation_marks(&self, scope: ScopeId) -> Vec<(ScopeId, Spelling)> {
        self.inner.borrow().scopes[scope.0 as usize]
            .mutation_marks
            .clone()
    }

    /// The evidence a scope with this origin inherits.
    ///
    /// Total over the origins, so a new kind of derived relation cannot
    /// quietly answer "unmarked" by not being listed. A relation naming
    /// another carries what it carries; a join carries BOTH arms', because
    /// a mutation whose source joins two marked relations does not say
    /// which one it meant. The roots carry nothing — a catalog access is
    /// where a mark is written, not where one is inherited.
    fn origin_mutation_marks(&self, origin: &ScopeOrigin) -> Vec<(ScopeId, Spelling)> {
        match *origin {
            ScopeOrigin::UserAlias { of }
            | ScopeOrigin::PipeStage { input: of }
            | ScopeOrigin::Wrap { input: of, .. }
            | ScopeOrigin::Cte { input: of, .. }
            | ScopeOrigin::SetArm { of, .. }
            | ScopeOrigin::ErHop { chain: of, .. } => self.mutation_marks(of),
            ScopeOrigin::Join { left, right } => {
                let mut marks = self.mutation_marks(left);
                for mark in self.mutation_marks(right) {
                    if !marks.iter().any(|(marked, _)| *marked == mark.0) {
                        marks.push(mark);
                    }
                }
                marks
            }
            ScopeOrigin::Interior { of } => self.mutation_marks(self.scope_of(of)),
            ScopeOrigin::BaseTable { .. }
            | ScopeOrigin::AnonRelation
            | ScopeOrigin::Resolution { .. }
            | ScopeOrigin::HoCarrier { .. }
            | ScopeOrigin::Scratch { .. } => Vec::new(),
        }
    }

    /// Record that this scope IS a relation the statement names directly and
    /// cannot alias.
    ///
    /// The emitted name follows from the entity, so it is not a spelling
    /// baptism may arbitrate: every other scope wanting those characters
    /// moves aside instead. A mutation's target is the only such scope
    /// today — `UPDATE employees … WHERE employees.id = …` writes the
    /// relation's name twice and the two have to be the same word.
    pub fn fix_relation_scope(&self, scope: ScopeId, entity: EntityId) {
        let canonical = self.inner.borrow().entities[entity.0 as usize].canonical;
        let mut inner = self.inner.borrow_mut();
        let record = &mut inner.scopes[scope.0 as usize];
        record.fixed_relation = true;
        record.emission_name = Some(canonical);
    }

    /// Whether this scope's emitted name is the relation's own.
    pub fn is_fixed_relation(&self, scope: ScopeId) -> bool {
        self.inner.borrow().scopes[scope.0 as usize].fixed_relation
    }

    /// Record that this scope offers rows chosen by position.
    ///
    /// The pass that bounds a relation says so here. Scopes minted from this
    /// one afterwards inherit it, which is why the mark has to land before
    /// the layers above are built — and it does: the bound is written where
    /// its restriction resolves, ahead of every scope downstream of it.
    pub fn mark_row_bounded(&self, scope: ScopeId) {
        self.inner.borrow_mut().scopes[scope.0 as usize].row_bound = true;
    }

    /// Whether this scope's rows were chosen by position.
    pub fn is_row_bounded(&self, scope: ScopeId) -> bool {
        self.inner.borrow().scopes[scope.0 as usize].row_bound
    }

    /// Whether a scope with this origin stands on bounded rows.
    ///
    /// Total over the origins, so a new kind of derived scope cannot quietly
    /// answer "unbounded" by not being listed. A relation naming another —
    /// an alias, a pipe stage, a wrap, a CTE binding, a set arm, an ER hop,
    /// an interior — offers whatever rows that one offers. A join offers a
    /// combination, so a bound on either side bounds the result. The roots
    /// bound nothing: a catalog access, a literal, a resolution scope, a
    /// higher-order carrier, and plan scratch each start from all their rows.
    fn origin_is_row_bounded(&self, origin: &ScopeOrigin) -> bool {
        match *origin {
            ScopeOrigin::UserAlias { of }
            | ScopeOrigin::PipeStage { input: of }
            | ScopeOrigin::Wrap { input: of, .. }
            | ScopeOrigin::Cte { input: of, .. }
            | ScopeOrigin::SetArm { of, .. }
            | ScopeOrigin::ErHop { chain: of, .. } => self.is_row_bounded(of),
            ScopeOrigin::Join { left, right } => {
                self.is_row_bounded(left) || self.is_row_bounded(right)
            }
            ScopeOrigin::Interior { of } => self.is_row_bounded(self.scope_of(of)),
            ScopeOrigin::BaseTable { .. }
            | ScopeOrigin::AnonRelation
            | ScopeOrigin::Resolution { .. }
            | ScopeOrigin::HoCarrier { .. }
            | ScopeOrigin::Scratch { .. } => false,
        }
    }

    /// A relation that exists without an enumerable heading.
    ///
    /// Identity and heading are different facts, and this is the case where
    /// the first is available and the second is not: a raw backend table
    /// nobody catalogued, a compiler wrap whose shape the pass minting it
    /// does not determine. It is not a relation with no columns, and the
    /// readers that need columns refuse rather than read zero.
    pub fn mint_opaque_scope(&self, origin: ScopeOrigin, hint: Hint) -> ScopeId {
        let scope = self.mint_scope(origin, hint, None);
        self.mark_heading_opaque(scope);
        scope
    }

    /// Record that a scope's dimensions cannot be enumerated.
    ///
    /// A generic relational call is the case: the target will publish rows
    /// this compiler has no description of. The scope still exists — it is
    /// the relation's identity — and only its heading is unknown.
    pub fn mark_heading_opaque(&self, scope: ScopeId) {
        let mut inner = self.inner.borrow_mut();
        let record = &mut inner.scopes[scope.0 as usize];
        debug_assert!(
            record.cols.is_empty(),
            "a scope that already published dimensions cannot become opaque"
        );
        record.opaque = true;
    }

    /// Mint a scope whose lexical parent follows directly from its origin.
    ///
    /// Compiler integration uses this path so a derived scope cannot name
    /// one input in its origin while recording an unrelated enclosure.
    pub fn mint_derived_scope(&self, origin: ScopeOrigin, hint: Hint) -> ScopeId {
        self.mint_scope(origin, hint, None)
    }

    /// The single authority relating a derived scope to its enclosure.
    fn derived_parent(&self, origin: &ScopeOrigin) -> Option<ScopeId> {
        match *origin {
            ScopeOrigin::UserAlias { of }
            | ScopeOrigin::PipeStage { input: of }
            | ScopeOrigin::Wrap { input: of, .. }
            | ScopeOrigin::Cte { input: of, .. }
            | ScopeOrigin::SetArm { of, .. } => Some(of),
            ScopeOrigin::ErHop { chain, .. } => Some(chain),
            ScopeOrigin::Interior { of } => Some(self.scope_of(of)),
            ScopeOrigin::BaseTable { .. }
            | ScopeOrigin::AnonRelation
            | ScopeOrigin::Join { .. }
            | ScopeOrigin::Resolution { .. }
            | ScopeOrigin::HoCarrier { .. }
            | ScopeOrigin::Scratch { .. } => None,
        }
    }

    /// Mint a column occurrence into a scope. Every argument is mandatory:
    /// a column with no stated addressing law is the state this model
    /// exists to make unrepresentable.
    pub fn mint_column(
        &self,
        into: ScopeId,
        origin: ColumnOrigin,
        published: Option<Spelling>,
        addressing: Addressing,
        facts: ValueFacts,
    ) -> ColId {
        let mut inner = self.inner.borrow_mut();
        let id = ColId(inner.cols.len() as u32);
        inner.cols.push(ColRecord {
            scope: into,
            origin,
            published,
            addressing,
            facts,
        });
        inner.scopes[into.0 as usize].cols.push(id);
        id
    }

    /// Mint and attach the interior relation owned by a column.
    ///
    /// The parent column must exist before `ScopeOrigin::Interior` can name
    /// it, while the column's value facts must point back to the resulting
    /// scope. Keeping that two-way link inside the registry makes the
    /// construction atomic to callers.
    pub fn mint_interior_scope(&self, of: ColId, hint: Hint) -> ScopeId {
        let parent = self.scope_of(of);
        let scope = self.mint_scope(ScopeOrigin::Interior { of }, hint, Some(parent));
        let mut inner = self.inner.borrow_mut();
        let facts = &mut inner.cols[of.0 as usize].facts;
        assert!(
            facts.interior.is_none(),
            "a column occurrence can own only one interior scope"
        );
        facts.interior = Some(scope);
        scope
    }

    /// Carry a whole heading across a scope boundary in one call.
    ///
    /// Each column becomes a NEW occurrence linked to the old one. Nothing
    /// is mutated, so a tree still holding the source columns keeps meaning
    /// what it meant.
    pub fn republish_heading(
        &self,
        from: ScopeId,
        into: ScopeId,
        how: Republish,
    ) -> Candidates<ColId> {
        let HeadingKnowledge::Known(source) = self.heading(from) else {
            // There are no occurrences to mint for dimensions nobody has
            // enumerated — and the destination inherits the reason. A scope
            // transition must not turn "the heading is unknown" into "the
            // heading has none".
            self.mark_heading_opaque(into);
            return Candidates::from_vec(Vec::new());
        };
        source
            .into_iter()
            .map(|c| {
                let (published, addressing, facts) = {
                    let inner = self.inner.borrow();
                    let r = &inner.cols[c.0 as usize];
                    (r.published, r.addressing, r.facts.clone())
                };
                self.mint_column(
                    into,
                    ColumnOrigin::Republished { from: c, how },
                    published,
                    addressing,
                    facts,
                )
            })
            .collect()
    }

    pub fn republish_column(
        &self,
        from: ColId,
        into: ScopeId,
        how: Republish,
        published: Option<Spelling>,
        addressing: Addressing,
        update_facts: impl FnOnce(&mut ValueFacts),
    ) -> ColId {
        let mut facts = self.facts(from);
        update_facts(&mut facts);
        self.mint_column(
            into,
            ColumnOrigin::Republished { from, how },
            published,
            addressing,
            facts,
        )
    }

    /// Republish a source occurrence for qualified lookup without adding a
    /// second heading slot.
    pub fn carry_qualified(&self, from: ColId, into: ScopeId) -> ColId {
        let published = self.published(from);
        let facts = self.facts(from);
        let mut inner = self.inner.borrow_mut();
        let id = ColId(inner.cols.len() as u32);
        inner.cols.push(ColRecord {
            scope: into,
            // A rider is the join keeping the occurrence USING merged away
            // reachable by its own qualifier, so it carries the same
            // provenance every other arm occurrence does.
            origin: ColumnOrigin::Republished {
                from,
                how: Republish::JoinArm,
            },
            published,
            addressing: Addressing::Hygienic,
            facts,
        });
        inner.scopes[into.0 as usize].qualified_carriers.push(id);
        id
    }

    /// Preserve every hidden qualified occurrence across another live join.
    pub fn carry_qualified_from(&self, from: ScopeId, into: ScopeId) {
        let carriers = self.inner.borrow().scopes[from.0 as usize]
            .qualified_carriers
            .clone();
        for carrier in carriers {
            self.carry_qualified(carrier, into);
        }
    }

    fn is_qualified_carrier(&self, column: ColId) -> bool {
        let inner = self.inner.borrow();
        let scope = inner.cols[column.0 as usize].scope;
        inner.scopes[scope.0 as usize]
            .qualified_carriers
            .contains(&column)
    }

    pub fn same_heading_names(&self, left: ScopeId, right: ScopeId) -> bool {
        let (HeadingKnowledge::Known(left), HeadingKnowledge::Known(right)) =
            (self.heading(left), self.heading(right))
        else {
            // Two headings nobody can enumerate do not answer "the same
            // names"; nothing has been compared.
            return false;
        };
        left.len() == right.len()
            && left
                .iter()
                .zip(right.iter())
                .all(|(left, right)| self.published_sym(*left) == self.published_sym(*right))
    }

    /// Build the ordered union of corresponding columns from several arms.
    pub fn merge_corresponding(
        &self,
        arms: &[ScopeId],
    ) -> Result<Option<ScopeId>, CorrespondenceError> {
        let Some(first) = arms.first().copied() else {
            return Ok(None);
        };
        let headings = arms
            .iter()
            .map(|arm| match self.heading(*arm) {
                HeadingKnowledge::Known(heading) => Ok(heading),
                HeadingKnowledge::Opaque => Err(CorrespondenceError::Opaque),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let output = self.mint_derived_scope(
            ScopeOrigin::Wrap {
                input: first,
                why: super::origin::WrapReason::SetOperation,
            },
            Hint::None,
        );
        let mut kept: Vec<ColId> = Vec::new();
        for heading in headings {
            let matched = self.corresponding_slots(&kept, &heading.to_vec())?;
            let matched_columns = matched.iter().flatten().copied().collect::<Vec<_>>();
            for (existing, column) in kept
                .iter()
                .copied()
                .zip(matched)
                .filter_map(|(existing, column)| column.map(|column| (existing, column)))
            {
                let left = self.facts(existing);
                let right = self.facts(column);
                let different_interiors = match (left.interior, right.interior) {
                    (Some(left), Some(right)) => !self.same_interior_shape(left, right),
                    _ => false,
                };
                if right.interior_conflict || different_interiors {
                    self.inner.borrow_mut().cols[existing.0 as usize]
                        .facts
                        .interior_conflict = true;
                }
            }
            for column in heading {
                if matched_columns.contains(&column) {
                    continue;
                }
                let (spelling, addressing, facts) = {
                    let inner = self.inner.borrow();
                    let record = &inner.cols[column.0 as usize];
                    (record.published, record.addressing, record.facts.clone())
                };
                let merged = self.mint_column(
                    output,
                    ColumnOrigin::Republished {
                        from: column,
                        how: Republish::ArmMerge,
                    },
                    spelling,
                    addressing,
                    facts,
                );
                kept.push(merged);
            }
        }
        Ok(Some(output))
    }

    fn same_interior_shape(&self, left: ScopeId, right: ScopeId) -> bool {
        let (HeadingKnowledge::Known(left), HeadingKnowledge::Known(right)) =
            (self.heading(left), self.heading(right))
        else {
            // Two headings nobody can enumerate are not the same shape; they
            // are two relations nothing is known about.
            return false;
        };
        left.len() == right.len()
            && left.iter().zip(right.iter()).all(|(left, right)| {
                if self.published_sym(*left) != self.published_sym(*right) {
                    return false;
                }
                match (self.facts(*left).interior, self.facts(*right).interior) {
                    (Some(left), Some(right)) => self.same_interior_shape(left, right),
                    (None, None) => true,
                    _ => false,
                }
            })
    }

    // ---- ask: never returns characters ---------------------------------

    pub fn scope_of(&self, c: ColId) -> ScopeId {
        self.inner.borrow().cols[c.0 as usize].scope
    }

    /// What this scope publishes, or that it cannot be said.
    pub fn heading(&self, s: ScopeId) -> HeadingKnowledge {
        let inner = self.inner.borrow();
        let record = &inner.scopes[s.0 as usize];
        if record.opaque {
            HeadingKnowledge::Opaque
        } else {
            HeadingKnowledge::Known(Candidates::from_vec(record.cols.clone()))
        }
    }

    /// The ordered dimensions, for a reader that needs structural column
    /// identities. Refuses on an opaque heading rather than answering empty.
    pub fn known_heading(&self, s: ScopeId) -> crate::error::Result<Candidates<ColId>> {
        self.heading(s).structural()
    }

    /// Whether any of these scopes has a heading nobody can enumerate.
    ///
    /// A reader about to report a name as NOT FOUND asks this first. "Not
    /// found" is a claim about an enumeration, and an opaque relation was
    /// never enumerated — saying a dimension is absent from it turns
    /// "unknown" into "not there".
    pub fn any_heading_opaque(&self, scopes: &[ScopeId]) -> bool {
        scopes.iter().any(|scope| self.heading(*scope).is_opaque())
    }

    pub fn common_scope(&self, columns: &[ColId]) -> Option<ScopeId> {
        let mut scopes = columns.iter().map(|column| self.scope_of(*column));
        let first = scopes.next()?;
        scopes.all(|scope| scope == first).then_some(first)
    }

    pub fn origin_of(&self, s: ScopeId) -> ScopeOrigin {
        self.inner.borrow().scopes[s.0 as usize].origin
    }

    pub fn origin_of_col(&self, c: ColId) -> ColumnOrigin {
        self.inner.borrow().cols[c.0 as usize].origin
    }

    /// Whether two entity handles denote one physical relation.
    ///
    /// One relation resolved twice in a statement can carry two handles —
    /// a DML target is minted beside the access its own source reads — so
    /// handle equality answers "the same lookup", not "the same table".
    /// The physical name is what the engine acts on, and it is what
    /// decides.
    pub fn same_relation(&self, a: EntityId, b: EntityId) -> bool {
        if a == b {
            return true;
        }
        let (left, right) = {
            let inner = self.inner.borrow();
            let left = &inner.entities[a.0 as usize];
            let right = &inner.entities[b.0 as usize];
            (
                (left.canonical, left.backend_schema),
                (right.canonical, right.backend_schema),
            )
        };
        self.canonical(left.0) == self.canonical(right.0)
            && left.1.map(|s| self.canonical(s)) == right.1.map(|s| self.canonical(s))
    }

    /// The catalog entity reached by a scope's complete origin chain.
    pub fn entity_of_scope(&self, scope: ScopeId) -> Option<EntityId> {
        fn find(
            registry: &Registry,
            scope: ScopeId,
            visited: &mut Vec<ScopeId>,
        ) -> Option<EntityId> {
            if visited.contains(&scope) {
                return None;
            }
            visited.push(scope);
            match registry.origin_of(scope) {
                ScopeOrigin::BaseTable { entity } | ScopeOrigin::Resolution { of: entity } => {
                    Some(entity)
                }
                ScopeOrigin::UserAlias { of }
                | ScopeOrigin::PipeStage { input: of }
                | ScopeOrigin::Wrap { input: of, .. }
                | ScopeOrigin::Cte { input: of, .. }
                | ScopeOrigin::SetArm { of, .. } => find(registry, of, visited),
                ScopeOrigin::ErHop { chain, .. } => find(registry, chain, visited),
                ScopeOrigin::Interior { of } => find(registry, registry.scope_of(of), visited),
                ScopeOrigin::Join { .. }
                | ScopeOrigin::AnonRelation
                | ScopeOrigin::HoCarrier { .. }
                | ScopeOrigin::Scratch { .. } => None,
            }
        }

        find(self, scope, &mut Vec::new())
    }

    pub fn parent_of(&self, s: ScopeId) -> Option<ScopeId> {
        self.inner.borrow().scopes[s.0 as usize].parent
    }

    /// Whether `candidate` contributes values to `scope`.
    ///
    /// The traversal follows every input edge declared by `ScopeOrigin`.
    /// Origins are append-only and can only point at earlier identities, but
    /// the visited set also makes the proof robust against malformed graphs.
    pub fn contains_scope(&self, scope: ScopeId, candidate: ScopeId) -> bool {
        fn contains(
            registry: &Registry,
            scope: ScopeId,
            candidate: ScopeId,
            visited: &mut Vec<ScopeId>,
        ) -> bool {
            if scope == candidate {
                return true;
            }
            if visited.contains(&scope) {
                return false;
            }
            visited.push(scope);
            let inputs: Vec<ScopeId> = match registry.origin_of(scope) {
                ScopeOrigin::UserAlias { of }
                | ScopeOrigin::PipeStage { input: of }
                | ScopeOrigin::Wrap { input: of, .. }
                | ScopeOrigin::Cte { input: of, .. }
                | ScopeOrigin::SetArm { of, .. } => vec![of],
                ScopeOrigin::Join { left, right } => vec![left, right],
                ScopeOrigin::ErHop { chain, .. } => vec![chain],
                ScopeOrigin::Interior { of } => vec![registry.scope_of(of)],
                ScopeOrigin::BaseTable { .. }
                | ScopeOrigin::AnonRelation
                | ScopeOrigin::Resolution { .. }
                | ScopeOrigin::HoCarrier { .. }
                | ScopeOrigin::Scratch { .. } => Vec::new(),
            };
            inputs
                .into_iter()
                .any(|input| contains(registry, input, candidate, visited))
        }

        contains(self, scope, candidate, &mut Vec::new())
    }

    pub fn answers_to(&self, s: ScopeId) -> Option<Sym> {
        self.answer_spelling(s)
            .map(|spelling| self.canonical(spelling))
    }

    pub fn published(&self, c: ColId) -> Option<Spelling> {
        self.inner.borrow().cols[c.0 as usize].published
    }

    pub fn published_sym(&self, c: ColId) -> Option<Sym> {
        self.published(c).map(|spelling| self.canonical(spelling))
    }

    pub fn addressing(&self, c: ColId) -> Addressing {
        self.inner.borrow().cols[c.0 as usize].addressing
    }

    pub fn facts(&self, c: ColId) -> ValueFacts {
        self.inner.borrow().cols[c.0 as usize].facts.clone()
    }

    /// Record that a cover gave this slot a new value.
    ///
    /// Said once, where the cover is resolved. A republication carries the
    /// fact because it carries the value's facts, so the occurrence a name,
    /// a projection or a boundary export hands on still answers yes — and a
    /// fresh read of the same catalog column, which shares no value, does
    /// not.
    pub fn mark_written_by_a_cover(&self, c: ColId) {
        self.inner.borrow_mut().cols[c.0 as usize]
            .facts
            .written_by_a_cover = true;
    }

    /// Whether what stands in this slot is a value being written.
    pub fn is_written_by_a_cover(&self, c: ColId) -> bool {
        self.inner.borrow().cols[c.0 as usize]
            .facts
            .written_by_a_cover
    }

    pub fn scope_answers(&self, scope: ScopeId, qualifier: Sym) -> bool {
        self.answers_to(scope) == Some(qualifier)
    }

    /// Expand a qualified glob under the tiers addressing uses — the same
    /// tiers, not tiers like them. `u.*` and `u.id` are one character apart
    /// and must reach the same columns, so both roads ask `reached_indirectly`
    /// and neither has a reach of its own.
    pub fn qualified_glob(&self, qualifier: Sym, candidates: &[ColId]) -> Candidates<ColId> {
        let candidates = self.with_qualified_carriers(candidates);
        let current: Vec<_> = candidates
            .iter()
            .copied()
            .filter(|column| self.scope_answers(self.scope_of(*column), qualifier))
            .collect();
        if !current.is_empty() {
            return Candidates::from_vec(current);
        }
        candidates
            .iter()
            .copied()
            .filter(|column| self.reached_indirectly(*column, qualifier))
            .collect()
    }

    fn with_qualified_carriers(&self, candidates: &[ColId]) -> Vec<ColId> {
        let mut expanded = candidates.to_vec();
        let mut scopes = Vec::new();
        for column in candidates {
            let scope = self.scope_of(*column);
            if !scopes.contains(&scope) {
                scopes.push(scope);
            }
        }
        let inner = self.inner.borrow();
        for scope in scopes {
            for carrier in &inner.scopes[scope.0 as usize].qualified_carriers {
                if !expanded.contains(carrier) {
                    expanded.push(*carrier);
                }
            }
        }
        expanded
    }

    /// Whether `qualifier` reaches a column by something other than naming the
    /// scope the column stands in — the fallback tier every qualified lookup
    /// shares.
    ///
    /// Two reaches. A column may itself answer under the qualifier, which is
    /// all that is left when an endpoints-only export lands it in a scope of
    /// the compiler's own. Or the qualifier still names an arm the statement
    /// reads through, which is what a join leaves standing.
    ///
    /// One place, because a glob and a single reference that disagreed about
    /// which columns a qualifier reaches would make `u.*` and `u.id` resolve
    /// against different sets.
    fn reached_indirectly(&self, column: ColId, qualifier: Sym) -> bool {
        let answering = matches!(
            self.addressing(column),
            Addressing::AnsweringTo(answer) | Addressing::BareAnswering(answer)
                if answer == qualifier
        );
        answering || self.came_through(column, qualifier)
    }

    /// Whether the occurrence a column is READ THROUGH sits in a scope
    /// answering to `qualifier`.
    ///
    /// Only a join is read through, and the occurrence says so: a join's
    /// republication is [`Republish::JoinArm`], recorded where the join made
    /// it. A join publishes a heading and carries no SQL alias, so its arms
    /// are still the FROM entries and `u` still names one of them —
    /// `users(*) as u, orders(*) as o, … |> (u.*)` asks about a table the
    /// statement still has. Every other boundary CONSUMED what it stood
    /// over: a projection ends its input's life, and a walk that reached past
    /// one would revive a scope the query no longer has, which is the defect
    /// `authored_dot_revives_consumed_scope` and
    /// `minted_dot_is_not_a_qualifier` are red for.
    ///
    /// The near end of the chain, not the far one: an arm aliased `u` sits
    /// between the join's occurrence and the base table, and asking the root
    /// would answer `users`. Two aliases of one table are told apart the same
    /// way, each reaching its own before either reaches what they share.
    fn came_through(&self, column: ColId, qualifier: Sym) -> bool {
        let mut cur = column;
        loop {
            if self.scope_answers(self.scope_of(cur), qualifier) {
                return true;
            }
            let ColumnOrigin::Republished { from, how } = self.origin_of_col(cur) else {
                return false;
            };
            if !how.consumes_nothing() {
                return false;
            }
            cur = from;
        }
    }

    /// The relation name that still reaches a column occurrence, if any —
    /// the answer [`came_through`](Self::came_through) tests for, computed
    /// instead of tested. A column already carrying an answer keeps it.
    /// Otherwise the nearest scope on the read-through chain that answers
    /// to a name answers for the column — an aliased join names itself
    /// before its arms, exactly as `came_through` has it.
    ///
    /// For a compiler wrap about to consume the scopes beside a column:
    /// this is the name the wrap owes the column, ridden as addressing,
    /// because after the wrap no scope is left to answer it.
    pub fn answering_reach(&self, column: ColId) -> Option<Sym> {
        if let answer @ Some(_) = self.answers_under(column) {
            return answer;
        }
        let mut cur = column;
        loop {
            if let Some(answer) = self.answers_to(self.scope_of(cur)) {
                return Some(answer);
            }
            let ColumnOrigin::Republished { from, how } = self.origin_of_col(cur) else {
                return None;
            };
            if !how.consumes_nothing() {
                return None;
            }
            cur = from;
        }
    }

    /// Regex selection remains inside the authority because the bound half's
    /// characters never leave the registry.
    pub fn pattern_columns(
        &self,
        pattern: &regex::Regex,
        candidates: &[ColId],
    ) -> Candidates<ColId> {
        candidates
            .iter()
            .copied()
            .filter(|column| {
                self.published(*column).is_some_and(|spelling| {
                    let (text, _) = self.spelling_text(spelling);
                    pattern.is_match(&text)
                })
            })
            .collect()
    }

    /// Expand a rename template without releasing the bound column spelling.
    pub fn expand_template(
        &self,
        source: ColId,
        template: &str,
        position: usize,
    ) -> Option<Spelling> {
        let source = self.published(source)?;
        let (text, stropped) = self.spelling_text(source);
        let expanded = template
            .replace("{#}", &position.to_string())
            .replace("{@}", &text);
        Some(self.intern(&expanded, stropped))
    }

    /// Return the spelling a column may safely publish through a qualified
    /// subquery boundary. SQLite parses `scope."true"` (and the analogous
    /// `false`/`null` forms) as a literal even though it is quoted, so those
    /// aliases need a distinct spelling before the outer scope addresses
    /// them.
    pub fn qualified_safe_spelling(&self, source: ColId) -> Option<Spelling> {
        let published = self.published(source)?;
        let (text, stropped) = self.spelling_text(published);
        if matches!(
            text.to_ascii_lowercase().as_str(),
            "true" | "false" | "null"
        ) {
            Some(self.intern(&format!("_{text}"), stropped))
        } else {
            Some(published)
        }
    }

    /// The relation a published column BELONGS to, as against the one that
    /// happens to be publishing it here.
    ///
    /// The two differ exactly at the boundaries that consume nothing. A join
    /// republishes both arms' headings into one scope so that the join has a
    /// heading to publish — but the arms are still standing, and each column
    /// is still one of theirs — and the emission wrap around a join operand
    /// is the same relation re-aliased for SQL syntax. Every other boundary
    /// consumed what it stood over, so there is no earlier relation left to
    /// belong to and the answer is the occurrence's own scope.
    ///
    /// What reads this is anything reporting WHICH relation a column is from
    /// — meta-ize's `scope` cell above all, where two anonymous relations
    /// joined together must not read as one.
    pub fn owner_of(&self, c: ColId) -> ScopeId {
        let mut cur = c;
        loop {
            match self.origin_of_col(cur) {
                ColumnOrigin::Republished { from, how } if how.consumes_nothing() => cur = from,
                _ => return self.scope_of(cur),
            }
        }
    }

    /// Walk the republication edges back to the column this value first was.
    pub fn progenitor(&self, c: ColId) -> ColId {
        let mut cur = c;
        loop {
            let next = match self.origin_of_col(cur) {
                ColumnOrigin::Republished { from, .. } => from,
                _ => return cur,
            };
            cur = next;
        }
    }

    /// Two occurrences carry the same value if they share a progenitor.
    pub fn same_value(&self, a: ColId, b: ColId) -> bool {
        self.progenitor(a) == self.progenitor(b)
    }

    /// Whether `candidate` stands downstream of `source` on the republication
    /// chain — that is, whether `candidate` is what a boundary made of it.
    ///
    /// Stricter than `same_value`, and the difference matters: two slots of one
    /// projection can share a progenitor while standing for different
    /// occurrences, and only the chain tells them apart.
    pub fn republishes(&self, candidate: ColId, source: ColId) -> bool {
        let mut cur = candidate;
        loop {
            if cur == source {
                return true;
            }
            match self.origin_of_col(cur) {
                ColumnOrigin::Republished { from, .. } => cur = from,
                _ => return false,
            }
        }
    }

    /// Find the one candidate occurrence corresponding to an output slot.
    ///
    /// A republication-chain connection is the strongest structural
    /// correspondence. An anonymous slot then falls back to shared value
    /// lineage; a named slot falls back to its published name.
    ///
    /// Every tier enumerates its whole candidate set before deciding whether
    /// it has zero, one, or many answers. A stronger unique tier settles the
    /// question without letting a weaker name collision conceal provenance.
    pub fn corresponding_slot(
        &self,
        output: ColId,
        candidates: &[ColId],
    ) -> Result<Option<ColId>, CorrespondenceError> {
        Ok(self
            .align_corresponding(&[output], candidates, false)?
            .into_iter()
            .next()
            .flatten())
    }

    /// Align two headings by correspondence without reusing an occurrence.
    ///
    /// Exact identity and republication chain are progressively weaker
    /// structural tiers. Anonymous slots then use shared value; named slots
    /// use published name. Each tier consumes only forced one-to-one pairs.
    /// Any remaining competing edges are ambiguity rather than permission to
    /// pick the first. Repeated compiler-published names are separate
    /// occurrences and align by occurrence rank; bare authored bindings with
    /// the same name refuse before that fallback.
    pub fn corresponding_slots(
        &self,
        outputs: &[ColId],
        candidates: &[ColId],
    ) -> Result<Vec<Option<ColId>>, CorrespondenceError> {
        self.align_corresponding(outputs, candidates, true)
    }

    fn align_corresponding(
        &self,
        outputs: &[ColId],
        candidates: &[ColId],
        rank_repeated_names: bool,
    ) -> Result<Vec<Option<ColId>>, CorrespondenceError> {
        self.refuse_duplicate_bound_names(outputs)?;
        self.refuse_duplicate_bound_names(candidates)?;
        let mut matches = vec![None; outputs.len()];
        let mut consumed = vec![false; candidates.len()];
        for tier in 0..4 {
            loop {
                let mut edges = Vec::new();
                for (output_index, output) in outputs.iter().copied().enumerate() {
                    if matches[output_index].is_some() {
                        continue;
                    }
                    for (candidate_index, candidate) in candidates.iter().copied().enumerate() {
                        if consumed[candidate_index] {
                            continue;
                        }
                        let corresponds = match tier {
                            0 => output == candidate,
                            1 => {
                                self.republishes(candidate, output)
                                    || self.republishes(output, candidate)
                            }
                            2 => {
                                self.published_sym(output).is_none()
                                    && self.same_value(output, candidate)
                            }
                            _ => {
                                let name = self.published_sym(output);
                                if name.is_none() || name != self.published_sym(candidate) {
                                    false
                                } else {
                                    let output_count = outputs
                                        .iter()
                                        .filter(|column| self.published_sym(**column) == name)
                                        .count();
                                    let candidate_count = candidates
                                        .iter()
                                        .filter(|column| self.published_sym(**column) == name)
                                        .count();
                                    if !rank_repeated_names
                                        || (output_count == 1 && candidate_count == 1)
                                    {
                                        true
                                    } else {
                                        let output_rank = outputs[..output_index]
                                            .iter()
                                            .filter(|column| self.published_sym(**column) == name)
                                            .count();
                                        let candidate_rank = candidates[..candidate_index]
                                            .iter()
                                            .filter(|column| self.published_sym(**column) == name)
                                            .count();
                                        output_rank == candidate_rank
                                    }
                                }
                            }
                        };
                        if corresponds {
                            edges.push((output_index, candidate_index));
                        }
                    }
                }
                if edges.is_empty() {
                    break;
                }
                let forced = edges
                    .iter()
                    .copied()
                    .filter(|(output_index, candidate_index)| {
                        edges
                            .iter()
                            .filter(|(other, _)| other == output_index)
                            .count()
                            == 1
                            && edges
                                .iter()
                                .filter(|(_, other)| other == candidate_index)
                                .count()
                                == 1
                    })
                    .collect::<Vec<_>>();
                if forced.is_empty() {
                    return Err(CorrespondenceError::Ambiguous);
                }
                for (output_index, candidate_index) in forced {
                    matches[output_index] = Some(candidates[candidate_index]);
                    consumed[candidate_index] = true;
                }
            }
        }
        Ok(matches)
    }

    fn refuse_duplicate_bound_names(&self, columns: &[ColId]) -> Result<(), CorrespondenceError> {
        for (index, column) in columns.iter().copied().enumerate() {
            if !matches!(
                self.addressing(column),
                Addressing::Bare | Addressing::BareAnswering(_)
            ) {
                continue;
            }
            let Some(name) = self.published_sym(column) else {
                continue;
            };
            if columns[index + 1..].iter().copied().any(|candidate| {
                matches!(
                    self.addressing(candidate),
                    Addressing::Bare | Addressing::BareAnswering(_)
                ) && self.scope_of(candidate) == self.scope_of(column)
                    && self.published_sym(candidate) == Some(name)
            }) {
                return Err(CorrespondenceError::Ambiguous);
            }
        }
        Ok(())
    }

    pub(super) fn reserved(&self) -> Vec<Sym> {
        self.inner.borrow().reserved.clone()
    }

    pub(super) fn canon_bytes(&self, s: Sym) -> Vec<u8> {
        self.inner.borrow().canon_text[s.0 as usize].clone()
    }

    pub(super) fn answer_spelling(&self, s: ScopeId) -> Option<Spelling> {
        self.inner.borrow().scopes[s.0 as usize].answers_to
    }

    pub(super) fn emission_prefix(&self, s: ScopeId) -> Option<&'static str> {
        self.inner.borrow().scopes[s.0 as usize].emission_prefix
    }

    pub(super) fn emission_name(&self, s: ScopeId) -> Option<Spelling> {
        self.inner.borrow().scopes[s.0 as usize].emission_name
    }

    pub(super) fn spelling_text(&self, s: Spelling) -> (String, bool) {
        let inner = self.inner.borrow();
        let r = &inner.spellings[s.0 as usize];
        (r.text.clone(), r.stropped)
    }

    // ---- the addressing authority --------------------------------------

    /// The only function that answers "does this reference address this
    /// column". It compares canonical identities, never characters.
    pub fn address(&self, r: Reference, env: &ScopeEnv) -> Result<ColId, AddressError> {
        // A scope whose dimensions the target never published contributes
        // no columns to any search, so a search that WOULD have reached it
        // did not reach everything — and every answer below, found or
        // absent or ambiguous, is a claim about the whole search. Which
        // searches it would have reached depends on the reference: an
        // unqualified one reaches every visible scope; a qualified one
        // reaches only the scopes that answer to the qualifier, so an
        // unrelated opaque relation being in view says nothing about it.
        let opaque: Vec<ScopeId> = env
            .visible
            .iter()
            .copied()
            .filter(|scope| self.heading(*scope).is_opaque())
            .collect();
        let mut visible: Vec<ColId> = match &env.candidates {
            Some(candidates) => candidates
                .iter()
                .copied()
                .filter(|c| env.visible.contains(&self.scope_of(*c)))
                .collect(),
            None => env
                .visible
                .iter()
                .flat_map(|s| match self.heading(*s) {
                    HeadingKnowledge::Known(heading) => heading.into_vec(),
                    HeadingKnowledge::Opaque => Vec::new(),
                })
                .collect(),
        };
        let Some(q) = r.qualifier else {
            // An unqualified reference reaches everything in view.
            if !opaque.is_empty() {
                return Err(AddressError::Incomplete);
            }
            let hits: Vec<ColId> = visible
                .into_iter()
                .filter(|c| self.answers_for(*c) == Some(r.name))
                .collect();
            return Candidates::from_vec(hits).settle(self);
        };
        {
            let inner = self.inner.borrow();
            for scope in &env.visible {
                for carrier in &inner.scopes[scope.0 as usize].qualified_carriers {
                    if !visible.contains(carrier) {
                        visible.push(*carrier);
                    }
                }
            }
        }

        // A qualifier names a scope. Failing that — and only then — it names
        // a relation a column still answers under: an endpoints-only export
        // lands its columns in a scope of the compiler's own, which answers to
        // nothing a user wrote, so the column is the only thing left that
        // knows. The two are tiers, not a union: where a scope does answer,
        // its own column and a downstream republication carrying the same
        // answer would both match, and one reference would name two columns.
        //
        // A tier is decided by whether it HAS the qualifier, not by whether it
        // then has the column. `q.x` where scope `q` is here and has no `x` is
        // a missing column, and falling through would answer it with whatever
        // unrelated occurrence still carries `q` in its addressing — binding a
        // different column under a name that was never wrong, only absent.
        if opaque
            .iter()
            .any(|scope| self.answers_to(*scope) == Some(q))
        {
            return Err(AddressError::Incomplete);
        }
        for named_by_scope in [true, false] {
            let mut hits: Vec<ColId> = Vec::new();
            let mut carriers = 0usize;
            for c in visible.iter().copied() {
                let carries = if named_by_scope {
                    self.answers_to(self.scope_of(c)) == Some(q)
                } else {
                    self.reached_indirectly(c, q)
                };
                if !carries {
                    continue;
                }
                carriers += 1;
                if self.answers_for(c) == Some(r.name)
                    || (self.is_qualified_carrier(c) && self.published_sym(c) == Some(r.name))
                {
                    hits.push(c);
                }
            }
            if carriers == 0 {
                continue;
            }
            return Candidates::from_vec(hits).settle(self);
        }
        // `_` is deixis, not a name: it selects the one visible anonymous
        // pipe stage — a relation that answers to nothing else a user could
        // write. A tier below the named ones, so an authored `_` (an alias,
        // a bound name) is never shadowed by the pointing form. Two visible
        // stages leave the reference ambiguous rather than picking one.
        // Read the spelling, never intern it: this is a lookup, and the
        // qualifier being compared was already interned at parse time — a
        // `_` no query wrote must not enter the spelling ledger here.
        if Some(q) == self.known_sym(ANONYMOUS_DEIXIS, false) {
            let mut stages: Vec<ScopeId> = Vec::new();
            for c in visible.iter().copied() {
                let s = self.scope_of(c);
                if self.answers_to(s).is_none()
                    && matches!(self.origin_of(s), ScopeOrigin::PipeStage { .. })
                    && !stages.contains(&s)
                {
                    stages.push(s);
                }
            }
            match stages.as_slice() {
                // Nothing to point at. Not a misspelled scope: `_` names
                // nothing, so there is no name to have got wrong.
                [] => return Err(AddressError::NoUnnamedPipe),
                [stage] => {
                    let hits: Vec<ColId> = visible
                        .iter()
                        .copied()
                        .filter(|c| {
                            self.scope_of(*c) == *stage && self.answers_for(*c) == Some(r.name)
                        })
                        .collect();
                    return Candidates::from_vec(hits).settle(self);
                }
                _ => return Err(AddressError::TwoUnnamedPipes),
            }
        }
        Err(AddressError::NoSuchScope)
    }

    /// Decide a tier's hits, after ruling out the ones that are not rivals.
    ///
    /// An occurrence and a republication of it downstream are ONE occurrence
    /// seen at two boundaries, not two columns. A join republishes each arm's
    /// heading, so a lookup offered the join's export beside the arm's own
    /// scope sees the same column twice; refusing there calls a query
    /// ambiguous that names exactly one value.
    ///
    /// The survivor is the UPSTREAM one — the occurrence the relation named by
    /// the qualifier owns. That is already what the scope tier hands back when
    /// an arm answers by name (`badges.emp_id` reads badges' own column, not
    /// the join's copy of it), and the two tiers answering one join differently
    /// would make `emp2.eid` and `badges.emp_id` stand at different boundaries
    /// in one condition.
    ///
    /// Only chain-relatedness collapses. Two occurrences that merely carry the
    /// same value — set-operation arms, a self-join's two sides — remain
    /// rivals, and the reference naming both is still ambiguous.
    /// What name, if any, a column answers to. `None` means nothing a user
    /// writes can reach it.
    fn answers_for(&self, c: ColId) -> Option<Sym> {
        match self.addressing(c) {
            Addressing::Published => self.published(c).map(|s| self.canonical(s)),
            Addressing::AnsweringTo(sym) => Some(sym),
            // An argumentative binding answers to the name the caller wrote
            // at the binding site: `users(uid, name)` is how this language
            // binds, so a bound lvar must be reachable by its own spelling.
            // `BareAnswering`'s Sym is the relation ALIAS, a qualifier — the
            // scope-level `answers_to` check consumes it; returning it here
            // as a column name makes `_(a, b) as t |> (t)` resolve.
            Addressing::Bare | Addressing::BareAnswering(_) => {
                self.published(c).map(|s| self.canonical(s))
            }
            Addressing::Hygienic | Addressing::Latent => None,
        }
    }

    /// What qualifier, if any, a column answers under on its own — beside
    /// whatever its scope answers to.
    ///
    /// A boundary that keeps a column's own name still records which relation
    /// it came through, and a reference may name that relation. When the
    /// column then lands in a scope of the compiler's own — a join's export,
    /// a wrap — there is no scope for the qualifier to name, and this is the
    /// only thing left that knows the answer.
    fn answers_under(&self, c: ColId) -> Option<Sym> {
        match self.addressing(c) {
            Addressing::AnsweringTo(sym) | Addressing::BareAnswering(sym) => Some(sym),
            Addressing::Published
            | Addressing::Bare
            | Addressing::Hygienic
            | Addressing::Latent => None,
        }
    }

    // ---- write: the only road to characters ----------------------------

    /// Spell a user-authored symbol into a sink.
    ///
    /// Compiler-minted scopes and columns are deliberately not spellable
    /// here — they have no name until baptism. A diagnostic that needs to
    /// talk about one gets [`describe`](Self::describe) instead, which is
    /// better teaching than a wrap alias would have been.
    /// The spelling AS AN IDENTIFIER: its characters with its strop bit.
    /// `SqlIdentifier`'s own equality is the identifier law, so this is the
    /// arena-free identity a map may be keyed by.
    pub fn identifier_of(&self, s: Spelling) -> delightql_types::SqlIdentifier {
        let inner = self.inner.borrow();
        let record = &inner.spellings[s.0 as usize];
        if record.stropped {
            delightql_types::SqlIdentifier::stropped(record.text.clone())
        } else {
            delightql_types::SqlIdentifier::new(record.text.clone())
        }
    }

    pub fn write<W: IdentSink>(&self, s: Spelling, w: &mut W) {
        let (text, stropped) = self.spelling_text(s);
        w.push_ident(&text, stropped);
    }

    /// Spell a scope reported as a VALUE — meta-ize puts one in every row it
    /// builds — never one written into the SQL text.
    ///
    /// Two cases, and only the first is settled here. A scope answering to an
    /// authored name reports THAT name: a [`Sym`] cannot be rendered, so the
    /// spelling is what is written, and the reader gets the characters they
    /// would have to type to qualify those columns. Baptism's alias is the
    /// wrong answer for it, being uniquified against whatever else one
    /// emission happens to name — two occurrences answering to `j` must both
    /// report `j` even when the SQL has to call one of them something else.
    ///
    /// A scope answering to nothing gets [`UNMINTED_MARKER`], which is a
    /// placeholder and not the answer. It is owed a name minted for that
    /// relation instead, and the marker's own documentation says why.
    /// Spell a never-named column reported as a VALUE, or decline.
    ///
    /// A column the user never named has no name to report: the emission's
    /// invented alias (`anon`, `op`) would name something no reference can
    /// address, which reads as an answer and is a lie. What DOES reach the
    /// column is its ordinal, so the report is that reference as the user
    /// would write it — `|2|`, or `t|2|` where the scope answers to a name.
    ///
    /// Declines (returns false) where no ordinal reaches the column either —
    /// a heading nobody enumerated has no position to point at.
    ///
    /// WHICH columns take this road is baptism's call, not this one's: an
    /// authored spelling that lost an ambiguity is emitted as an invented
    /// name too, and the registry still holds the characters its author
    /// wrote.
    pub fn write_ordinal_report<W: IdentSink>(&self, c: ColId, w: &mut W) -> bool {
        let scope = self.scope_of(c);
        let HeadingKnowledge::Known(heading) = self.heading(scope) else {
            return false;
        };
        let Some(position) = heading.iter().position(|col| *col == c) else {
            return false;
        };
        // The qualifier travels as an IDENTIFIER, carrying its stropped bit,
        // and the ordinal syntax follows as its own plain segment. Flattening
        // the two into one string drops the bit, and `a b|1|` is not a
        // reference anyone can type — the very thing this report promises.
        if let Some(spelling) = self.answer_spelling(scope) {
            self.write(spelling, w);
        }
        w.push_ident(&format!("|{}|", position + 1), false);
        true
    }

    /// Spell a catalog entity's physical relation name into a sink.
    pub fn write_entity<W: IdentSink>(&self, entity: EntityId, w: &mut W) {
        let (backend_schema, canonical) = {
            let inner = self.inner.borrow();
            let record = &inner.entities[entity.0 as usize];
            (record.backend_schema, record.canonical)
        };
        if let Some(schema) = backend_schema {
            self.write(schema, w);
            w.push_ident(".", false);
        }
        self.write(canonical, w);
    }

    pub fn write_function<W: IdentSink>(
        &self,
        function: FnId,
        w: &mut W,
    ) -> Result<(), FunctionSpellingError> {
        if let FnOrigin::Intrinsic(intrinsic) = self.function_origin(function) {
            if intrinsic.canonical().is_none() {
                return Err(FunctionSpellingError::NoCanonicalSpelling { intrinsic });
            }
        }
        self.write_function_namespace(function, w);
        self.write_function_name(function, w)
    }

    /// Spell only the namespace portion of a function, including its
    /// trailing separator.
    ///
    /// SQL generation needs this separately because a dialect rule may
    /// replace the call name or the whole call shape. The characters still
    /// leave through the same sealed sink.
    pub fn write_function_namespace<W: IdentSink>(&self, function: FnId, w: &mut W) {
        let namespace = {
            let inner = self.inner.borrow();
            let record = &inner.functions[function.0 as usize];
            record.namespace.clone()
        };
        for schema in namespace.into_iter().rev() {
            self.write(schema, w);
            w.push_ident(".", false);
        }
    }

    /// Spell only the base name of a function.
    pub fn write_function_name<W: IdentSink>(
        &self,
        function: FnId,
        w: &mut W,
    ) -> Result<(), FunctionSpellingError> {
        let (origin, authored) = {
            let inner = self.inner.borrow();
            let record = &inner.functions[function.0 as usize];
            (record.origin, record.authored)
        };
        match origin {
            FnOrigin::User(_) => self.write(
                authored.expect("a user function must retain its authored spelling"),
                w,
            ),
            FnOrigin::Intrinsic(intrinsic) => {
                let spelling = intrinsic
                    .canonical()
                    .ok_or(FunctionSpellingError::NoCanonicalSpelling { intrinsic })?;
                w.push_ident(spelling, false);
            }
        }
        Ok(())
    }

    /// The scopes a reference may name, given one that is live.
    ///
    /// A join is READ THROUGH: it publishes a heading and carries no name of
    /// its own, so what a qualifier reaches is its arms — the same law
    /// [`came_through`](Self::came_through) resolves by. Telling a reader
    /// that `a join` is in scope answers a question about `t` and `u` with
    /// the one word they already knew.
    ///
    /// A join the user aliased names itself and its arms are behind it.
    /// Anything else is its own answer: a pipe stage consumed what it stood
    /// over, so its input is no longer nameable.
    pub fn nameable_scopes(&self, s: ScopeId) -> Vec<ScopeId> {
        let mut out = Vec::new();
        self.collect_nameable(s, &mut out);
        out
    }

    fn collect_nameable(&self, s: ScopeId, out: &mut Vec<ScopeId>) {
        if self.answer_spelling(s).is_none() {
            if let ScopeOrigin::Join { left, right } = self.origin_of(s) {
                self.collect_nameable(left, out);
                self.collect_nameable(right, out);
                return;
            }
        }
        if !out.contains(&s) {
            out.push(s);
        }
    }

    /// Describe a scope structurally, for diagnostics before baptism.
    pub fn describe<W: IdentSink>(&self, s: ScopeId, w: &mut W) {
        match self.answer_spelling(s) {
            Some(spelling) => self.write(spelling, w),
            None => {
                let word = match self.origin_of(s) {
                    ScopeOrigin::BaseTable { .. } => "a table access",
                    ScopeOrigin::UserAlias { .. } => "an alias",
                    ScopeOrigin::AnonRelation => "an anonymous relation",
                    ScopeOrigin::Join { .. } => "a join",
                    ScopeOrigin::PipeStage { .. } => "a pipe stage",
                    ScopeOrigin::Wrap { .. } => "a compiler wrap",
                    ScopeOrigin::Cte { .. } => "a with-binding",
                    ScopeOrigin::SetArm { .. } => "a set-operation arm",
                    ScopeOrigin::Resolution { .. } => "a resolution scope",
                    ScopeOrigin::ErHop { .. } => "a relationship hop",
                    ScopeOrigin::HoCarrier { .. } => "a higher-order carrier",
                    ScopeOrigin::Scratch { .. } => "a scratch table",
                    ScopeOrigin::Interior { .. } => "an interior relation",
                };
                w.push_ident(word, false);
            }
        }
    }
}
