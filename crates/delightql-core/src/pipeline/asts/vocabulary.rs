//! The identity and closed-vocabulary types the core AST is built from.
//!
//! What lives here is what the production tree in `asts/` USES: the written
//! reference and what it resolves to, the marks and modes that are part of a
//! name, the non-empty vector, the uninhabited payload, and the closed
//! operator vocabularies. Each type has exactly one home, and it is this one
//! — a second copy beside the tree would be free to disagree with it.
//!
//! Nothing here is a relation carrier. The relational shapes — chains,
//! grelexes, continuations, calls, heads — live in `asts/core`, which is the
//! tree the compiler runs; a parallel skeleton restating them is a
//! representation the pipeline never reads and cannot be kept honest by
//! anything the pipeline does.
//!
//! Construction-fence witnesses are crate-local compile-fail probes. They
//! exercise prohibited operations from inside the crate so private module
//! boundaries do not become the accidental diagnostic; the probe feature is
//! never enabled by ordinary builds or doctests.

use crate::names::{CallableId, Registry, Spelling};
use std::rc::Rc;

// ---------------------------------------------------------------------------
// Foundations
// ---------------------------------------------------------------------------

/// A non-empty vector. There is no public constructor from a possibly-empty
/// `Vec`; emptiness is unspellable, not checked (a namespace path has at
/// least one segment; an access with no slots is a different access).
#[derive(Debug, Clone, PartialEq)]
pub struct Vec1<T> {
    head: T,
    tail: Vec<T>,
}

impl<T: crate::lispy::ToLispy> crate::lispy::ToLispy for Vec1<T> {
    fn to_lispy(&self) -> String {
        let items = self
            .iter()
            .map(crate::lispy::ToLispy::to_lispy)
            .collect::<Vec<_>>()
            .join(" ");
        format!("[{}]", items)
    }
}

impl<T> Vec1<T> {
    pub fn new(head: T) -> Self {
        Vec1 {
            head,
            tail: Vec::new(),
        }
    }
    pub fn with_tail(head: T, tail: Vec<T>) -> Self {
        Vec1 { head, tail }
    }
    /// The only door from a possibly-empty vector, and it is fallible.
    pub fn try_from_vec(mut v: Vec<T>) -> Option<Self> {
        if v.is_empty() {
            return None;
        }
        let head = v.remove(0);
        Some(Vec1 { head, tail: v })
    }
    pub fn len(&self) -> usize {
        1 + self.tail.len()
    }
    pub fn first(&self) -> &T {
        &self.head
    }
    pub fn get(&self, index: usize) -> Option<&T> {
        match index {
            0 => Some(&self.head),
            n => self.tail.get(n - 1),
        }
    }
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        std::iter::once(&self.head).chain(self.tail.iter())
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        std::iter::once(&mut self.head).chain(self.tail.iter_mut())
    }

    pub fn into_vec(self) -> Vec<T> {
        let mut out = Vec::with_capacity(self.len());
        out.push(self.head);
        out.extend(self.tail);
        out
    }

    /// The first member and the rest. Non-emptiness is what makes this
    /// total: a consumer that treats a single member specially reads that
    /// member by proof, not by indexing a vector it hopes is short.
    pub fn into_head_tail(self) -> (T, Vec<T>) {
        (self.head, self.tail)
    }

    /// Combine every member into one, left to right. There is no empty case
    /// for a caller to invent a meaning for.
    pub fn reduce(self, mut combine: impl FnMut(T, T) -> T) -> T {
        let (head, tail) = self.into_head_tail();
        tail.into_iter().fold(head, &mut combine)
    }

    /// Pair with another non-empty collection of the SAME length. Answers
    /// `None` when the lengths differ rather than stopping at the shorter
    /// one: a zip that silently truncates turns a width error into a wrong
    /// answer instead of a refusal.
    pub fn zip_exact<U>(self, other: Vec1<U>) -> Option<Vec1<(T, U)>> {
        if self.len() != other.len() {
            return None;
        }
        Some(Vec1 {
            head: (self.head, other.head),
            tail: self.tail.into_iter().zip(other.tail).collect(),
        })
    }

    /// Non-emptiness survives a per-element rewrite, so mapping needs no
    /// second fallible door back in.
    pub fn map<U>(self, mut f: impl FnMut(T) -> U) -> Vec1<U> {
        Vec1 {
            head: f(self.head),
            tail: self.tail.into_iter().map(f).collect(),
        }
    }

    /// `map` for a rewrite that can refuse. The count is unchanged either
    /// way, so a fallible walk never has to re-prove non-emptiness.
    pub fn try_map<U, E>(self, mut f: impl FnMut(T) -> Result<U, E>) -> Result<Vec1<U>, E> {
        let head = f(self.head)?;
        let mut tail = Vec::with_capacity(self.tail.len());
        for item in self.tail {
            tail.push(f(item)?);
        }
        Ok(Vec1 { head, tail })
    }
}

impl<T> std::ops::Index<usize> for Vec1<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        self.get(index).expect("Vec1 index out of range")
    }
}

impl<'a, T> IntoIterator for &'a Vec1<T> {
    type Item = &'a T;
    type IntoIter = Box<dyn Iterator<Item = &'a T> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

/// A vector of AT LEAST TWO. Where a production says "two or more", the
/// carrier says it too: there is no public constructor from a possibly-short
/// `Vec`, so a one-member or empty value of that production is unspellable
/// rather than checked.
#[derive(Debug, Clone, PartialEq)]
pub struct Vec2<T> {
    first: T,
    second: T,
    rest: Vec<T>,
}

impl<T: crate::lispy::ToLispy> crate::lispy::ToLispy for Vec2<T> {
    fn to_lispy(&self) -> String {
        let items = self
            .iter()
            .map(crate::lispy::ToLispy::to_lispy)
            .collect::<Vec<_>>()
            .join(" ");
        format!("[{}]", items)
    }
}

impl<T> Vec2<T> {
    pub fn new(first: T, second: T) -> Self {
        Vec2 {
            first,
            second,
            rest: Vec::new(),
        }
    }

    /// The one door from a possibly-short vector, and it is fallible. The
    /// CST boundary is where the count is proved; nothing downstream reproves
    /// it.
    pub fn try_from_vec(mut v: Vec<T>) -> Option<Self> {
        if v.len() < 2 {
            return None;
        }
        let mut drain = v.drain(..);
        let first = drain.next().expect("length checked");
        let second = drain.next().expect("length checked");
        let rest = drain.collect();
        Some(Vec2 {
            first,
            second,
            rest,
        })
    }

    pub fn len(&self) -> usize {
        2 + self.rest.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        std::iter::once(&self.first)
            .chain(std::iter::once(&self.second))
            .chain(self.rest.iter())
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        std::iter::once(&mut self.first)
            .chain(std::iter::once(&mut self.second))
            .chain(self.rest.iter_mut())
    }

    pub fn into_vec(self) -> Vec<T> {
        let mut out = Vec::with_capacity(self.len());
        out.push(self.first);
        out.push(self.second);
        out.extend(self.rest);
        out
    }

    /// The first member and the rest. At-least-two is what makes this
    /// total, and it is how an at-least-two collection becomes an
    /// at-least-one collection without a fallible door.
    pub fn into_head_tail(self) -> (T, Vec<T>) {
        let mut tail = Vec::with_capacity(1 + self.rest.len());
        tail.push(self.second);
        tail.extend(self.rest);
        (self.first, tail)
    }

    /// At-least-two survives a per-element rewrite, so mapping needs no
    /// second fallible door back in.
    pub fn map<U>(self, mut f: impl FnMut(T) -> U) -> Vec2<U> {
        Vec2 {
            first: f(self.first),
            second: f(self.second),
            rest: self.rest.into_iter().map(f).collect(),
        }
    }

    /// The same, for a rewrite that can refuse.
    pub fn try_map<U, E>(self, mut f: impl FnMut(T) -> Result<U, E>) -> Result<Vec2<U>, E> {
        let first = f(self.first)?;
        let second = f(self.second)?;
        let mut rest = Vec::with_capacity(self.rest.len());
        for item in self.rest {
            rest.push(f(item)?);
        }
        Ok(Vec2 {
            first,
            second,
            rest,
        })
    }
}

impl<'a, T> IntoIterator for &'a Vec2<T> {
    type Item = &'a T;
    type IntoIter = Box<dyn Iterator<Item = &'a T> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

/// Uninhabited. A phase-exclusive payload becomes `Never` after its phase:
/// no value can be constructed, and the match arm need not be written at all.
///
/// The derives carry the bounds a phase payload must satisfy. Each one is
/// reachable only through a value of this type, so each is a `match` with
/// no arms: the compiler proves the body cannot run rather than a panic
/// standing in for the proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Never {}

impl crate::lispy::ToLispy for Never {
    fn to_lispy(&self) -> String {
        match *self {}
    }
}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

/// The written reference. Built at exactly one boundary; there is no
/// constructor from a string containing `::` — the builder's decode is the
/// only decode. The origin is part of the type, so authored parsing and
/// compiler synthesis can never share a back door.
#[derive(Clone)]
pub struct Ref {
    registry: Rc<Registry>,
    ns: Namespace,
    name: Spelling,
    mark: Mark,
    resolution: ResolutionMode,
    origin: RefOrigin,
}

impl Ref {
    /// The authored door: the one CST-boundary decode.
    pub fn written(
        registry: Rc<Registry>,
        ns: Namespace,
        name: Spelling,
        mark: Mark,
        resolution: ResolutionMode,
    ) -> Self {
        Ref {
            registry,
            ns,
            name,
            mark,
            resolution,
            origin: RefOrigin::Authored,
        }
    }
    /// The synthesis door. Every compiler-created reference names its reason
    /// explicitly, so authored and generated identities cannot share a
    /// construction path.
    pub fn synthetic(
        registry: Rc<Registry>,
        reason: SyntheticReason,
        ns: Namespace,
        name: Spelling,
    ) -> Self {
        Ref {
            registry,
            ns,
            name,
            mark: Mark::Plain,
            resolution: ResolutionMode::Normal,
            origin: RefOrigin::Synthetic(reason),
        }
    }

    pub(crate) fn synthetic_with_display(
        registry: &Rc<Registry>,
        reason: SyntheticReason,
        name: &str,
    ) -> Self {
        Self::synthetic(
            Rc::clone(registry),
            reason,
            Namespace::Ambient,
            registry.intern(name, false),
        )
    }
    pub fn origin(&self) -> &RefOrigin {
        &self.origin
    }
    pub fn namespace(&self) -> &Namespace {
        &self.ns
    }
    pub fn name(&self) -> &Spelling {
        &self.name
    }
    pub fn mark(&self) -> Mark {
        self.mark
    }
    pub fn resolution(&self) -> &ResolutionMode {
        &self.resolution
    }

    /// The callee's name as the identifier it is — characters plus strop
    /// bit, read through the reference's OWN registry, so the identity is
    /// portable across arenas.
    pub(crate) fn name_identifier(&self) -> delightql_types::SqlIdentifier {
        self.registry.identifier_of(self.name)
    }

    pub(crate) fn name_text(&self) -> String {
        let mut text = String::new();
        self.registry
            .write(self.name, &mut crate::names::sink::Teaching(&mut text));
        text
    }

    pub(crate) fn namespace_texts(&self) -> Vec<String> {
        match &self.ns {
            Namespace::Ambient => Vec::new(),
            Namespace::Path(path) => path
                .iter()
                .map(|spelling| {
                    let mut text = String::new();
                    self.registry
                        .write(*spelling, &mut crate::names::sink::Teaching(&mut text));
                    text
                })
                .collect(),
        }
    }

    pub(crate) fn namespace_fq(&self) -> Option<String> {
        let namespace = self.namespace_texts();
        (!namespace.is_empty()).then(|| namespace.join("::"))
    }
}

impl PartialEq for Ref {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.registry, &other.registry)
            && self.registry.canonical(self.name) == self.registry.canonical(other.name)
            && namespace_eq(&self.registry, &self.ns, &other.ns)
            && self.mark == other.mark
            && self.resolution == other.resolution
            && self.origin == other.origin
    }
}

fn namespace_eq(registry: &Registry, left: &Namespace, right: &Namespace) -> bool {
    match (left, right) {
        (Namespace::Ambient, Namespace::Ambient) => true,
        (Namespace::Path(left), Namespace::Path(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right.iter())
                    .all(|(left, right)| registry.canonical(*left) == registry.canonical(*right))
        }
        _ => false,
    }
}

impl std::fmt::Debug for Ref {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Ref")
            .field("ns", &self.ns)
            .field("name", &self.name)
            .field("mark", &self.mark)
            .field("resolution", &self.resolution)
            .field("origin", &self.origin)
            .finish()
    }
}

impl crate::lispy::ToLispy for Ref {
    fn to_lispy(&self) -> String {
        let name = self.name_text();
        let qualified = match &self.ns {
            Namespace::Ambient => name,
            Namespace::Path(_) => format!("{}.{name}", self.namespace_texts().join("::")),
        };
        format!(
            "(ref {} {:?} {:?})",
            crate::lispy::ToLispy::to_lispy(&qualified),
            self.mark,
            self.resolution
        )
    }
}

/// Where a reference came from.
#[derive(Debug, Clone, PartialEq)]
pub enum RefOrigin {
    Authored,
    Synthetic(SyntheticReason),
}

/// Enumerated compiler synthesis sites. Adding a new site requires naming its
/// reason here, keeping generated references auditable at the type boundary.
#[derive(Debug, Clone, PartialEq)]
pub enum SyntheticReason {
    EffectReceipt,
    RowNumber,
}

/// `Ambient` is "nothing written" — a value, not a hole: non-optional is not
/// non-empty. Segments are ordinary identifiers, classic or stropped —
/// stroppedness is spelling, never meaning. There is no `Rooted` variant:
/// the leading `::` is not a namespace root; the light mention owns that
/// spelling.
#[derive(Debug, Clone, PartialEq)]
pub enum Namespace {
    Ambient,
    Path(Vec1<Spelling>),
}

/// Semantic reference routing selected by the authored namespace separator.
/// The CST may preserve the character; the AST preserves only its meaning.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolutionMode {
    Normal,
    TargetPassthrough,
}

/// The marks that are part of a NAME. The `!` is effect identity (`stdout!`
/// IS the name); a trailing `::` is the catalog functor (`main::(*)`). The
/// `!!` mutation marker is NOT here: there is no entity named `emp!!` — `!!`
/// is call-site evidence and lives only in `FunctorMarks.mutation`. One
/// authored fact, one field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mark {
    Plain,
    Effect,
    Catalog,
}

impl Ref {
    /// The callable identity of a written call — what the reference IS
    /// after resolution. The namespace does not survive resolution; a
    /// resolved callee is one thing, and the handle exposes no character
    /// API, so code that reaches for a spelling after resolution does not
    /// compile.
    ///
    /// The authored name becomes a function identity. Nothing is looked up
    /// and nothing refuses: DelightQL does not require a catalog entry to
    /// call something, and an unrecognised name is the target's to
    /// interpret. The identity carries the spelling so the generator can
    /// write it back out.
    pub fn written_call_identity(&self, registry: &Registry) -> CallableId {
        let spelling = registry.intern(&self.name_text(), false);
        let namespace = self
            .namespace_texts()
            .into_iter()
            .map(|part| registry.intern(&part, false))
            .collect();
        registry.mint_function(spelling, namespace)
    }
}

/// The authored badge lives with THE BINDING AUTHORITY, which owns the walk
/// that judges it, the binding it decides, and the mint of the deduplicating
/// outcome's evidence. It is named here because a head carries it as
/// `CteAuthority::fixpoint`; it is not defined here, because a second place
/// able to construct the decision would be a second authority.
pub use crate::pipeline::bindings::Fixpoint;

// ---------------------------------------------------------------------------
// Call-site and step evidence
// ---------------------------------------------------------------------------

/// Index of a bag-op arm within its chain. Arm 0 is the chain the run stands
/// on; the refiner that writes one and the chain that counts them agree
/// because there is one type saying what is being counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArmIx(u16);

impl ArmIx {
    pub fn from_raw(value: u16) -> Self {
        ArmIx(value)
    }

    pub fn value(self) -> u16 {
        self.0
    }
}

impl crate::lispy::ToLispy for ArmIx {
    fn to_lispy(&self) -> String {
        self.0.to_string()
    }
}

/// Call-site evidence: the outer `?` and the mutation `!!`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FunctorMarks {
    outer: bool,
    mutation: bool,
}

impl FunctorMarks {
    pub fn with_evidence(outer: bool, mutation: bool) -> Self {
        FunctorMarks { outer, mutation }
    }

    pub fn outer(&self) -> bool {
        self.outer
    }
    pub fn mutation(&self) -> bool {
        self.mutation
    }
}

// ---------------------------------------------------------------------------
// The closed operator vocabularies
// ---------------------------------------------------------------------------

/// The closed arithmetic vocabulary. Total functions only: no `Option` that
/// "unknown" can ride into "no parentheses needed".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Concat,
}

/// The comparison vocabulary accepted by the predicate grammar. The parser
/// owns the string-to-authority decode; later phases carry this enum rather
/// than rediscovering operator spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    NullSafeEqual,
    NullSafeNotEqual,
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
}

impl BinOp {
    /// The operator's one written name, shared by diagnostics and the lispy
    /// rendering. The GLYPH is the parser's; this is the vocabulary's.
    pub fn name(self) -> &'static str {
        match self {
            Self::Add => "add",
            Self::Sub => "subtract",
            Self::Mul => "multiply",
            Self::Div => "divide",
            Self::Mod => "modulo",
            Self::Concat => "concat",
        }
    }
}

impl crate::lispy::ToLispy for BinOp {
    fn to_lispy(&self) -> String {
        self.name().to_string()
    }
}

impl crate::lispy::ToLispy for CmpOp {
    fn to_lispy(&self) -> String {
        self.sql_name().to_string()
    }
}

impl CmpOp {
    /// Decode the complete comparison vocabulary at a syntax boundary,
    /// accepting both the written operator and the grammar's node name.
    /// Internal phases carry this enum and never reparse spelling.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "null_safe_eq" | "IS NOT DISTINCT FROM" => Some(Self::NullSafeEqual),
            "null_safe_ne" | "IS DISTINCT FROM" => Some(Self::NullSafeNotEqual),
            "=" | "traditional_eq" => Some(Self::Equal),
            "!=" | "traditional_ne" => Some(Self::NotEqual),
            "<" | "less_than" => Some(Self::LessThan),
            ">" | "greater_than" => Some(Self::GreaterThan),
            "<=" | "less_than_eq" => Some(Self::LessThanOrEqual),
            ">=" | "greater_than_eq" => Some(Self::GreaterThanOrEqual),
            _ => None,
        }
    }

    pub fn sql_name(self) -> &'static str {
        match self {
            Self::NullSafeEqual => "IS NOT DISTINCT FROM",
            Self::NullSafeNotEqual => "IS DISTINCT FROM",
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::LessThan => "<",
            Self::GreaterThan => ">",
            Self::LessThanOrEqual => "<=",
            Self::GreaterThanOrEqual => ">=",
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::names::Registry;
    use std::collections::BTreeSet;

    fn public_visibility(visibility: &syn::Visibility) -> bool {
        matches!(visibility, syn::Visibility::Public(_))
    }

    fn collect_public_shape(
        items: &[syn::Item],
        types: &mut Vec<String>,
        variants: &mut Vec<String>,
        public_fields: &mut Vec<String>,
    ) {
        for item in items {
            match item {
                syn::Item::Struct(item) if public_visibility(&item.vis) => {
                    let name = item.ident.to_string();
                    types.push(format!("struct {name}"));
                    match &item.fields {
                        syn::Fields::Named(fields) => {
                            for field in &fields.named {
                                if public_visibility(&field.vis) {
                                    public_fields.push(format!(
                                        "{name}::{}",
                                        field.ident.as_ref().expect("named field")
                                    ));
                                }
                            }
                        }
                        syn::Fields::Unnamed(fields) => {
                            for (index, field) in fields.unnamed.iter().enumerate() {
                                if public_visibility(&field.vis) {
                                    public_fields.push(format!("{name}::{index}"));
                                }
                            }
                        }
                        syn::Fields::Unit => {}
                    }
                }
                syn::Item::Enum(item) if public_visibility(&item.vis) => {
                    let name = item.ident.to_string();
                    types.push(format!("enum {name}"));
                    variants.extend(
                        item.variants
                            .iter()
                            .map(|variant| format!("{name}::{}", variant.ident)),
                    );
                }
                syn::Item::Trait(item) if public_visibility(&item.vis) => {
                    types.push(format!("trait {}", item.ident));
                }
                syn::Item::Type(item) if public_visibility(&item.vis) => {
                    types.push(format!("type {}", item.ident));
                }
                syn::Item::Mod(item) => {
                    if let Some((_, nested)) = &item.content {
                        collect_public_shape(nested, types, variants, public_fields);
                    }
                }
                _ => {}
            }
        }
    }

    fn assert_unique(label: &str, entries: &[String]) {
        let unique = entries.iter().collect::<BTreeSet<_>>();
        assert_eq!(
            entries.len(),
            unique.len(),
            "duplicate {label}: {entries:?}"
        );
    }

    /// The inventory is the ratchet: a relation carrier reappearing here is
    /// a second tree, and this test is where that shows up as a failure
    /// rather than as a module that quietly regrows.
    #[test]
    fn generated_type_and_variant_inventory_is_current() {
        let file =
            syn::parse_file(include_str!("vocabulary.rs")).expect("the vocabulary module parses");
        let mut types = Vec::new();
        let mut variants = Vec::new();
        let mut public_fields = Vec::new();
        collect_public_shape(&file.items, &mut types, &mut variants, &mut public_fields);
        types.sort();
        variants.sort();
        public_fields.sort();

        let expected_types: &[&str] = &[
            "enum BinOp",
            "enum CmpOp",
            "enum Mark",
            "enum Namespace",
            "enum Never",
            "enum RefOrigin",
            "enum ResolutionMode",
            "enum SyntheticReason",
            "struct ArmIx",
            "struct FunctorMarks",
            "struct Ref",
            "struct Vec1",
            "struct Vec2",
        ];
        let expected_variants: &[&str] = &[
            "BinOp::Add",
            "BinOp::Concat",
            "BinOp::Div",
            "BinOp::Mod",
            "BinOp::Mul",
            "BinOp::Sub",
            "CmpOp::Equal",
            "CmpOp::GreaterThan",
            "CmpOp::GreaterThanOrEqual",
            "CmpOp::LessThan",
            "CmpOp::LessThanOrEqual",
            "CmpOp::NotEqual",
            "CmpOp::NullSafeEqual",
            "CmpOp::NullSafeNotEqual",
            "Mark::Catalog",
            "Mark::Effect",
            "Mark::Plain",
            "Namespace::Ambient",
            "Namespace::Path",
            "RefOrigin::Authored",
            "RefOrigin::Synthetic",
            "ResolutionMode::Normal",
            "ResolutionMode::TargetPassthrough",
            "SyntheticReason::EffectReceipt",
            "SyntheticReason::RowNumber",
        ];

        assert_unique("public type", &types);
        assert_unique("public enum variant", &variants);
        assert_eq!(
            types.iter().map(String::as_str).collect::<Vec<_>>(),
            expected_types
        );
        assert_eq!(
            variants.iter().map(String::as_str).collect::<Vec<_>>(),
            expected_variants
        );
        assert!(
            public_fields.is_empty(),
            "public AST fields: {public_fields:?}"
        );
    }

    #[test]
    fn synthetic_refs_share_registry_identity() {
        let registry = Rc::new(Registry::new(&[]));
        let json = Ref::synthetic_with_display(&registry, SyntheticReason::EffectReceipt, "json");
        let count = Ref::synthetic_with_display(&registry, SyntheticReason::EffectReceipt, "count");
        let json_again =
            Ref::synthetic_with_display(&registry, SyntheticReason::EffectReceipt, "json");

        assert_ne!(json, count);
        assert_eq!(json, json_again);

        let other_registry = Rc::new(Registry::new(&[]));
        let json_elsewhere =
            Ref::synthetic_with_display(&other_registry, SyntheticReason::EffectReceipt, "json");
        assert_ne!(json, json_elsewhere);
    }

    #[test]
    fn ref_lispy_renders_qualified_spelling() {
        let registry = Rc::new(Registry::new(&[]));
        let namespace = Namespace::Path(Vec1::with_tail(
            registry.intern("lib", false),
            vec![registry.intern("math", false)],
        ));
        let reference = Ref::written(
            Rc::clone(&registry),
            namespace,
            registry.intern("apply", false),
            Mark::Plain,
            ResolutionMode::Normal,
        );

        assert_eq!(
            crate::lispy::ToLispy::to_lispy(&reference),
            "(ref \"lib::math.apply\" Plain Normal)"
        );
    }

    /// A non-empty vector has no empty door.
    #[test]
    fn an_empty_vec1_is_unspellable() {
        assert!(Vec1::<Spelling>::try_from_vec(Vec::new()).is_none());
    }

    /// Namespace is Ambient or a non-empty path; there is no Rooted variant
    /// (this test is the absence, written down: the match below is
    /// exhaustive with two arms).
    #[test]
    fn namespace_has_exactly_two_states() {
        let registry = Rc::new(Registry::new(&[]));
        let ns = Namespace::Path(Vec1::new(registry.intern("t", false)));
        let arms = match ns {
            Namespace::Ambient => 1,
            Namespace::Path(_) => 2,
        };
        assert_eq!(arms, 2);
    }
}
