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
    Addressing, FnOrigin, FunctionSpellingError, Hint, Intrinsic, ScopeKind, ValueFacts,
};
use super::sink::IdentSink;

/// The spelling that POINTS at the one visible anonymous pipe stage.
///
/// One authored spelling and the canonical identity it folds to.
struct SpellingRecord {
    text: String,
    stropped: bool,
    canon: Sym,
}

struct ScopeRecord {
    kind: ScopeKind,
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
    /// Physical column occurrences admitted into this scope, retained only
    /// for the final SQL naming sweep. Semantic lookup and interface laws
    /// cannot reach this inventory.
    late_columns: Vec<ColId>,
    /// This scope is emitted as a relation the statement cannot alias, so
    /// its name is the relation's and is not available for arbitration.
    ///
    /// A mutation's target is the case: the statement writes the physical
    /// relation name and every reference to its columns must render the
    /// same characters, so a rename would leave the two disagreeing.
    fixed_relation: bool,
    /// A positional reference reached one of this scope's columns. For an
    /// inchoate occurrence that reach is ACTIVATION: position reaches what
    /// names cannot, so the occurrence yields its rows.
    ordinal_reached: bool,
    /// An unaccessed inchoate occurrence: nothing activated it, so it
    /// lowers to zero rows under its opaque displayed heading.
    annihilated: bool,
    /// This occurrence is a TRUTH WITNESS (`+`/`\+`): it answers to the
    /// relation it probes so a correlation can address it, but it is not a
    /// live row-space relation — the existence overlap is a truth, and a
    /// semijoin is a lowering. Scope activation's duplicate-answering
    /// judgment skips it.
    truth_witness: bool,
}

struct ColRecord {
    /// THE qualifier fact, in one copy.
    scope: ScopeId,
    /// The spelling the user should see, if it has one. `None` means the
    /// column is compiler-anonymous and baptism will name it.
    published: Option<Spelling>,
    addressing: Addressing,
    facts: ValueFacts,
    /// Position at admission, used only to report an authored ordinal after
    /// late naming. This is not a relation-interface reconstruction road.
    ordinal: u32,
    /// This occurrence's authored name LOST AN AMBIGUITY: its heading
    /// published the same canonical name at another position too.
    ///
    /// The spelling stays, because the heading that holds the ambiguity
    /// still reports it as the qualified ordinal that reaches it. What
    /// the mark governs is what happens NEXT: authored-name loss is
    /// monotonic along the slot lineage, so carrying the occurrence across
    /// a boundary carries no name, and only a new authored naming act
    /// gives the position a name again.
    name_lost: bool,
}

struct EntityRecord {
    canonical: Spelling,
    backend_schema: Option<Spelling>,
}

/// One catalog storage object, independent of how many entity handles looked
/// it up. Constructed only from the catalog identity already bound to the
/// entity; consumers cannot substitute a spelling or backend schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CatalogStorageKey {
    canonical: Sym,
    backend_schema: Option<Sym>,
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
    /// Canonical identities of every ADMITTED authored name in this
    /// compilation — recorded at position admission, read by baptism so a
    /// drawn invention can never collide with a name an author owns
    /// (ALIAS ALWAYS PRE-EMPTS A MINT).
    authored_reserved: Vec<Sym>,
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
    /// What each relation of this compilation publishes.
    ///
    /// It lives here because a `Registry` IS one compilation, and it is
    /// OPAQUE here: every method on it is private to `crate::relation`, so
    /// holding this object gives a caller no way to record a relation, read
    /// an interface, or close the epoch. The registry owns its lifetime and
    /// the authority owns its meaning.
    relations: crate::relation::RelationStore,
    /// Where this compilation's semantic ports meet the columns its SQL
    /// emits.
    ///
    /// Here for the reason `relations` is here, and OPAQUE the same way:
    /// a binding elaborates semantic evidence, so the two cannot come from
    /// two epochs, and every method on this object is private to
    /// `crate::sql_binding`.
    bindings: crate::sql_binding::SqlBindingMap,
    /// This compilation's ONE live scope environment.
    ///
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
        let relations = crate::relation::RelationStore::new();
        let bindings =
            crate::sql_binding::SqlBindingMap::new(crate::relation::epoch_of(&relations));
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
                authored_reserved: Vec::new(),
            }),
            limits: crate::compiler_limits::ArmedLimits::in_force(),
            relations,
            bindings,
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

    /// This compilation's relation records.
    ///
    /// Handing out the object is not handing out a capability: nothing on
    /// it is callable outside `crate::relation`.
    pub(crate) fn relations(&self) -> &crate::relation::RelationStore {
        &self.relations
    }

    /// This compilation's physical bindings.
    ///
    /// Handing out the object is not handing out a capability: nothing on
    /// it is callable outside `crate::sql_binding`.
    pub(crate) fn bindings(&self) -> &crate::sql_binding::SqlBindingMap {
        &self.bindings
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
    /// How many scopes this compilation has minted.
    ///
    /// A COUNT, for the one property a caller can check about minting
    /// without naming what was minted: that a refused act left nothing.
    #[cfg(test)]
    pub(crate) fn scopes_minted(&self) -> usize {
        self.inner.borrow().scopes.len()
    }

    pub(super) fn mint_scope(
        &self,
        kind: ScopeKind,
        hint: Hint,
        parent: Option<ScopeId>,
    ) -> ScopeId {
        let (answers_to, emission_prefix, emission_name) = match hint {
            Hint::User(sp) => (Some(sp), None, None),
            Hint::Prefix(prefix) => (None, Some(prefix), None),
            Hint::Exact(sp) => (None, None, Some(sp)),
            Hint::None => (None, None, None),
        };
        let mut inner = self.inner.borrow_mut();
        let id = ScopeId(inner.scopes.len() as u32);
        inner.scopes.push(ScopeRecord {
            kind,
            emission_prefix,
            emission_name,
            parent,
            answers_to,
            late_columns: Vec::new(),
            fixed_relation: false,
            ordinal_reached: false,
            annihilated: false,
            truth_witness: false,
        });
        id
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

    /// Record that this occurrence is a truth witness (`+`/`\+`): nameable
    /// for correlation, never a live row-space relation.
    pub fn mark_truth_witness(&self, scope: ScopeId) {
        self.inner.borrow_mut().scopes[scope.0 as usize].truth_witness = true;
    }

    pub fn is_truth_witness(&self, scope: ScopeId) -> bool {
        self.inner.borrow().scopes[scope.0 as usize].truth_witness
    }

    /// AUTHORED-NAME LOSS IS MONOTONIC. Seal one finished heading: any
    /// canonical name it publishes at more than one position is an
    /// ambiguity, and every position holding it loses the name.
    ///
    /// The spelling itself stays on the occurrence, because the heading
    /// that HOLDS the ambiguity still reports each position as the
    /// qualified ordinal that reaches it — the one licensed ordinal
    /// report. What the seal governs is the future: a later boundary
    /// carrying one of these occurrences carries no name with it, so a
    /// projection that leaves one position standing cannot recover a name
    /// the repetition took away. Only a new authored naming act can.
    pub fn seal_heading_ambiguities(&self, heading: &[ColId]) {
        let mut names: Vec<(Sym, usize)> = Vec::new();
        for column in heading {
            let Some(name) = self.published_sym(*column) else {
                continue;
            };
            match names.iter_mut().find(|(seen, _)| *seen == name) {
                Some((_, count)) => *count += 1,
                None => names.push((name, 1)),
            }
        }
        let contested: Vec<Sym> = names
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .map(|(name, _)| name)
            .collect();
        if contested.is_empty() {
            return;
        }
        let mut inner = self.inner.borrow_mut();
        for column in heading {
            let record = &inner.cols[column.0 as usize];
            let Some(published) = record.published else {
                continue;
            };
            let canonical = inner.spellings[published.0 as usize].canon;
            if contested.contains(&canonical) {
                inner.cols[column.0 as usize].name_lost = true;
            }
        }
    }

    /// Carry the loss to a republication of a marked occurrence.
    pub(super) fn inherit_name_loss(&self, column: ColId) {
        self.inner.borrow_mut().cols[column.0 as usize].name_lost = true;
    }

    /// Whether this occurrence's authored name lost an ambiguity.
    pub(super) fn name_lost(&self, column: ColId) -> bool {
        self.inner.borrow().cols[column.0 as usize].name_lost
    }

    /// The same, for the projection authority: a position holding a name
    /// the repetition already took away is not the author naming it again.
    pub fn name_lost_to_ambiguity(&self, column: ColId) -> bool {
        self.name_lost(column)
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

    /// Mint a column occurrence into a scope. Every argument is mandatory:
    /// a column with no stated addressing law is the state this model
    /// exists to make unrepresentable.
    fn mint_column(
        &self,
        into: ScopeId,
        published: Option<Spelling>,
        addressing: Addressing,
        facts: ValueFacts,
    ) -> ColId {
        let mut inner = self.inner.borrow_mut();
        let id = ColId(inner.cols.len() as u32);
        let ordinal = inner.scopes[into.0 as usize].late_columns.len() as u32;
        inner.cols.push(ColRecord {
            scope: into,
            published,
            addressing,
            facts,
            ordinal,
            name_lost: false,
        });
        inner.scopes[into.0 as usize].late_columns.push(id);
        id
    }

    /// Allocate a physical SQL alias after semantic construction has sealed.
    ///
    /// The source contributes spelling and value metadata only. No semantic
    /// ancestry or ownership edge is recorded; those facts live in the
    /// relation store and SQL binding map.
    pub(crate) fn rebind_sql_column(
        &self,
        source: ColId,
        into: ScopeId,
        published: Option<Spelling>,
    ) -> ColId {
        self.mint_column(into, published, self.addressing(source), self.facts(source))
    }

    pub(crate) fn mint_semantic_port(
        &self,
        _authority: &crate::relation::builder::SemanticConstruction,
        source: ColId,
        into: ScopeId,
        published: Option<Spelling>,
        addressing: Addressing,
        update: impl FnOnce(&mut ValueFacts),
    ) -> ColId {
        let mut facts = self.facts(source);
        update(&mut facts);
        let carried = published == self.published(source);
        let output = self.mint_column(into, published, addressing, facts);
        if carried && self.name_lost(source) {
            self.inherit_name_loss(output);
        }
        output
    }

    pub(crate) fn mint_new_semantic_port(
        &self,
        _authority: &crate::relation::builder::SemanticConstruction,
        into: ScopeId,
        published: Option<Spelling>,
        addressing: Addressing,
        facts: ValueFacts,
    ) -> ColId {
        self.mint_column(into, published, addressing, facts)
    }

    pub(crate) fn sql_column(
        &self,
        into: ScopeId,
        published: Option<Spelling>,
        addressing: Addressing,
    ) -> ColId {
        self.mint_column(into, published, addressing, ValueFacts::default())
    }

    /// THE IDENTITY OF A ROW POSITION THAT NAMES NOTHING.
    ///
    /// The compiler's own values in a row — an existence probe's `SELECT 1`,
    /// a receipt's constant, a crossed truth — still OCCUPY a column: width,
    /// ordering, wrapping and dialect rewrites all act on it. So each one
    /// has an identity, even though no reference addresses it and no `AS` is
    /// printed for it. It publishes no spelling, which is what keeps it out
    /// of every heading that is compared.
    pub(crate) fn scaffolding_slot(&self) -> ColId {
        let at = self.anonymous_scope(None);
        self.sql_column(at, None, Addressing::Hygienic)
    }

    /// Mint the lexical scope used to emit an interior relation.
    /// Semantic ownership is recorded by the relation authority, not as a
    /// copied scope identity in a value-facts sidecar.
    pub(super) fn mint_interior_scope(&self, of: ColId, hint: Hint) -> ScopeId {
        let parent = self.scope_of(of);
        self.inner.borrow_mut().cols[of.0 as usize]
            .facts
            .tree_valued = true;
        self.mint_scope(ScopeKind::Interior, hint, Some(parent))
    }

    // ---- ask: never returns characters ---------------------------------

    pub fn scope_of(&self, c: ColId) -> ScopeId {
        self.inner.borrow().cols[c.0 as usize].scope
    }

    /// Physical occurrences gathered for the final naming pass. This has no
    /// semantic consumer and makes no claim about a relation interface.
    pub(crate) fn late_naming_columns(&self, scope: ScopeId) -> Vec<ColId> {
        self.inner.borrow().scopes[scope.0 as usize]
            .late_columns
            .clone()
    }

    pub fn common_scope(&self, columns: &[ColId]) -> Option<ScopeId> {
        let mut scopes = columns.iter().map(|column| self.scope_of(*column));
        let first = scopes.next()?;
        scopes.all(|scope| scope == first).then_some(first)
    }

    pub fn kind_of(&self, s: ScopeId) -> ScopeKind {
        self.inner.borrow().scopes[s.0 as usize].kind
    }

    pub(crate) fn catalog_storage_key(&self, entity: EntityId) -> CatalogStorageKey {
        let inner = self.inner.borrow();
        let entity = &inner.entities[entity.0 as usize];
        CatalogStorageKey {
            canonical: inner.spellings[entity.canonical.0 as usize].canon,
            backend_schema: entity
                .backend_schema
                .map(|schema| inner.spellings[schema.0 as usize].canon),
        }
    }

    pub fn parent_of(&self, s: ScopeId) -> Option<ScopeId> {
        self.inner.borrow().scopes[s.0 as usize].parent
    }

    /// THE AUTHORED OWNER OF A STAGE. `|> … as s` names the relation the
    /// stage produced: an identity fact of that occurrence, reported by the
    /// metadata view and recorded here once. It is not a route — which
    /// authored spelling reaches the relation at a position is the lexical
    /// frontier's judgment — and it is written by the crossing that
    /// consumed the stage's input, over the relation that crossing
    /// produced, never over a relation a caller chose. A scope that already
    /// answers to a name cannot be renamed by it.
    pub(crate) fn adopt_stage_owner(
        &self,
        scope: ScopeId,
        answer: Spelling,
    ) -> Result<(), crate::error::DelightQLError> {
        {
            let mut inner = self.inner.borrow_mut();
            let record = &mut inner.scopes[scope.0 as usize];
            if record.answers_to.is_some() {
                return Err(crate::error::DelightQLError::transformation_error(
                    "a stage that already answers to a name cannot be named again",
                    "stage owner",
                ));
            }
            record.answers_to = Some(answer);
        }
        Ok(())
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

    pub(crate) fn is_tree_valued(&self, c: ColId) -> bool {
        self.inner.borrow().cols[c.0 as usize].facts.tree_valued
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

    /// Refuse where ONE scope binds one name twice argumentatively.
    ///
    /// Owed by every correspondence road, the set law's included: there the
    /// author wrote an ambiguity no ranking resolves, so no alignment may
    /// pick one of the two.
    pub(crate) fn refuse_duplicate_bound_names(
        &self,
        columns: &[ColId],
    ) -> Result<(), CorrespondenceError> {
        for (index, column) in columns.iter().copied().enumerate() {
            if !matches!(
                self.addressing(column),
                Addressing::Bare | Addressing::BareUnder
            ) {
                continue;
            }
            let Some(name) = self.published_sym(column) else {
                continue;
            };
            if columns[index + 1..].iter().copied().any(|candidate| {
                matches!(
                    self.addressing(candidate),
                    Addressing::Bare | Addressing::BareUnder
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

    /// Record an ADMITTED authored name's canonical identity, so baptism
    /// draws no invention that collides with it. Called by the position
    /// admission authority (`names::identifier`); interning here is
    /// deliberate — an authored name is a use, not a read-only probe.
    pub(crate) fn reserve_authored(&self, text: &str, stropped: bool) {
        let spelling = self.intern(text, stropped);
        let sym = self.canonical(spelling);
        let mut inner = self.inner.borrow_mut();
        if !inner.authored_reserved.contains(&sym) {
            inner.authored_reserved.push(sym);
        }
    }

    /// The canonical identities of every admitted authored name.
    pub(super) fn authored_reserved(&self) -> Vec<Sym> {
        self.inner.borrow().authored_reserved.clone()
    }

    pub(super) fn canon_bytes(&self, s: Sym) -> Vec<u8> {
        self.inner.borrow().canon_text[s.0 as usize].clone()
    }

    /// TWO LIVE SCOPES NEVER SHARE A NAME, judged over one co-visible set
    /// of scopes the lexical frontier made addressable together. The
    /// answer each scope was born under — or adopted as a stage owner — is
    /// the registry's record; no capability is minted to ask it.
    pub(crate) fn refuse_shared_names(
        &self,
        co_visible: &[ScopeId],
        policy: super::scope::DuplicateScopePolicy,
    ) -> Result<(), crate::error::DelightQLError> {
        let mut seen: Vec<(Sym, ScopeId)> = Vec::with_capacity(co_visible.len());
        for scope in co_visible.iter().copied() {
            let Some(answer) = self.answers_to(scope) else {
                continue;
            };
            if seen
                .iter()
                .any(|(name, owner)| *name == answer && *owner != scope)
            {
                match policy {
                    super::scope::DuplicateScopePolicy::Acknowledged => {}
                    super::scope::DuplicateScopePolicy::Refuse => {
                        let spelling = self
                            .answer_spelling(scope)
                            .map(|spelling| self.spelling_text(spelling).0)
                            .unwrap_or_default();
                        return Err(super::scope::ScopeActivationRefusal::DuplicateAnswer {
                            spelling,
                        }
                        .into());
                    }
                }
            }
            seen.push((answer, scope));
        }
        Ok(())
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
        let inner = self.inner.borrow();
        let column = &inner.cols[c.0 as usize];
        let scope = column.scope;
        let position = column.ordinal;
        drop(inner);
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

    /// The namespace parts a callable was written under, outermost first.
    /// Empty for a bare citation and for every intrinsic.
    pub fn function_namespace(&self, function: FnId) -> Vec<Spelling> {
        let inner = self.inner.borrow();
        inner.functions[function.0 as usize].namespace.clone()
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

    /// Describe a scope structurally, for diagnostics before baptism.
    pub fn describe<W: IdentSink>(&self, s: ScopeId, w: &mut W) {
        match self.answer_spelling(s) {
            Some(spelling) => self.write(spelling, w),
            None => {
                let word = match self.kind_of(s) {
                    ScopeKind::BaseTable { .. } => "a table access",
                    ScopeKind::UserAlias { .. } => "an alias",
                    ScopeKind::AnonRelation => "an anonymous relation",
                    ScopeKind::Join { .. } => "a join",
                    ScopeKind::PipeStage { .. } => "a pipe stage",
                    ScopeKind::Wrap { .. } => "a compiler wrap",
                    ScopeKind::Cte { .. } => "a with-binding",
                    ScopeKind::SetArm { .. } => "a set-operation arm",
                    ScopeKind::Resolution { .. } => "a resolution scope",
                    ScopeKind::ErHop { .. } => "a relationship hop",
                    ScopeKind::HoCarrier { .. } => "a higher-order carrier",
                    ScopeKind::Scratch { .. } => "a scratch table",
                    ScopeKind::Interior => "an interior relation",
                };
                w.push_ident(word, false);
            }
        }
    }
}
