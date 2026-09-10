// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The machinery under the diagnostic hierarchy: what `#[derive(Taxon)]`
//! writes against, and what every boundary reads from.
//!
//! A diagnostic is a value of the nested enums in [`crate::diagnostic`]. From
//! that one value this module derives the payload-free identity
//! ([`ErrorId`]), the transport-neutral [`DiagnosticClass`], the registry
//! descriptor, and the rendered URI. Nothing here accepts a hierarchy string
//! from production: segments are literals in the taxonomy declaration and
//! reach this module only through the derive.
//!
//! Family matching ([`ErrorSelector::matches`]) compares the identity's
//! typed segment view with a selector validated against the same declared
//! tree. No rendered URI is parsed to answer a family question.

use std::fmt;

/// The identifier badge scheme for compiler-minted error identities:
/// `delightql-error://<hierarchy>` ⇌
/// `https://delightql.org/uri/error/<hierarchy>`.
pub const ERROR_URI_SCHEME: &str = "delightql-error://";

/// The transport-neutral class of a diagnostic: what KIND of failure it is,
/// independent of the compiler phase that noticed it. The protocol boundary
/// converts this exhaustively to its wire enum; no producer chooses a wire
/// class beside an identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DiagnosticClass {
    /// The program is malformed or semantically invalid: it cannot mean
    /// anything and must be rewritten.
    Syntax,
    /// A stated expectation or data constraint did not hold.
    Constraint,
    /// The system, engine, or transport failed operationally.
    Connection,
    /// Refused by policy or by a check the statement may not run without.
    Permission,
    /// The engine gave up waiting.
    Timeout,
}

/// One family node of the hierarchy, as the derive declares it.
pub struct FamilyDescriptor {
    pub segment: &'static str,
    pub summary: &'static str,
    pub explanation: &'static str,
    pub children: &'static [Node],
    /// A provider-owned open tail under this family, when the family is an
    /// external root.
    pub external: Option<&'static ExternalTail>,
}

/// One emitted identity, as the derive declares it. `segments` is relative
/// to the enclosing family; empty means the leaf IS the family's own emitted
/// identity.
pub struct LeafDescriptor {
    pub segments: &'static [&'static str],
    /// `None` only for a provider-owned terminal, whose class the provider
    /// derives from its native code.
    pub class: Option<DiagnosticClass>,
    pub summary: &'static str,
    pub explanation: &'static str,
}

/// A provider-owned open tail: the validator that admits a dynamic segment
/// list, the class the provider derives from an admitted tail, the root's
/// own descriptor, and the prose the registry shows for the root.
pub struct ExternalTail {
    pub validate: fn(&[&str]) -> bool,
    pub class_of_tail: fn(&[&str]) -> Option<DiagnosticClass>,
    pub descriptor: &'static LeafDescriptor,
    pub summary: &'static str,
    pub explanation: &'static str,
}

/// A child of a family node.
pub enum Node {
    Family(&'static FamilyDescriptor),
    Leaf(&'static LeafDescriptor),
}

/// What a `#[carrier]` variant holds: an occurrence whose identity the tree
/// already declared and answered for — never one the carrier invented.
pub trait Carried {
    fn push_segments(&self, out: &mut Vec<&'static str>);
    fn tail(&self) -> Vec<String>;
    fn leaf(&self) -> &'static LeafDescriptor;
    fn class(&self) -> DiagnosticClass;
}

/// What the derive implements for every enum of the hierarchy.
pub trait Taxon {
    /// This enum's children, in declaration order.
    const CHILDREN: &'static [Node];
    /// This enum's provider-owned open tail, if it has one.
    const EXTERNAL: Option<&'static ExternalTail>;
    /// Append the segments this occurrence contributes, then its child's.
    fn push_segments(&self, out: &mut Vec<&'static str>);
    /// The validated dynamic segments of a provider-owned terminal; empty
    /// for every fixed identity.
    fn tail(&self) -> Vec<String>;
    /// The leaf descriptor this occurrence is.
    fn leaf(&self) -> &'static LeafDescriptor;
    /// The class of this occurrence.
    fn class(&self) -> DiagnosticClass;
}

/// A provider-owned terminal: a validated native code space under an
/// external root. Only the provider's own validated constructors build one.
pub trait ExternalTerminal {
    /// Whether an authored selector tail is a lawful path in this space.
    fn validate_tail(tail: &[&str]) -> bool;
    /// The class a complete, validated tail determines; `None` for a tail
    /// that names a class alone or is not a terminal.
    fn class_of_tail(tail: &[&str]) -> Option<DiagnosticClass>;
    /// The dynamic segments this occurrence renders under its root.
    fn tail(&self) -> Vec<String>;
    /// The class the native code determines.
    fn class(&self) -> DiagnosticClass;
}

/// The payload-free identity of one diagnostic occurrence: the segments the
/// taxonomy declared for its leaf, plus a provider-owned tail when the leaf
/// is an external terminal. Derived from an occurrence, never constructed
/// beside one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ErrorId {
    fixed: Vec<&'static str>,
    tail: Vec<String>,
}

impl ErrorId {
    pub(crate) fn of<T: Taxon>(occurrence: &T) -> ErrorId {
        let mut fixed = Vec::new();
        occurrence.push_segments(&mut fixed);
        ErrorId {
            fixed,
            tail: occurrence.tail(),
        }
    }

    /// An identity the declared tree answered for: fixed segments are the
    /// tree's own literals, the tail a provider-validated one. Only
    /// [`Received::decode`] builds one this way.
    fn declared(fixed: Vec<&'static str>, tail: Vec<String>) -> ErrorId {
        ErrorId { fixed, tail }
    }

    /// The declared (fixed) segments, root first.
    pub fn fixed_segments(&self) -> &[&'static str] {
        &self.fixed
    }

    /// Every segment, fixed then provider-owned, root first.
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.fixed
            .iter()
            .copied()
            .chain(self.tail.iter().map(String::as_str))
    }

    /// The bare hierarchy: `semantic/resolution/column`.
    pub fn hierarchy(&self) -> String {
        self.segments().collect::<Vec<_>>().join("/")
    }

    /// The badge form: `delightql-error://semantic/resolution/column`. The
    /// one rendering, for display, storage, protocol, and external APIs.
    pub fn uri(&self) -> String {
        format!("{ERROR_URI_SCHEME}{}", self.hierarchy())
    }

    /// The top family segment.
    pub fn top(&self) -> &'static str {
        self.fixed.first().copied().unwrap_or("")
    }
}

impl fmt::Display for ErrorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.uri())
    }
}

// ----------------------------------------------------------------------------
// The declared tree: ONE projection, read by the inventory, by selector
// admission, and by received-identity decoding
// ----------------------------------------------------------------------------

/// The declared role of one registry hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// A matchable family node; not itself emitted.
    Family,
    /// An emitted identity.
    Leaf,
    /// A family whose own path is also an emitted identity.
    FamilyAndLeaf,
    /// A family whose exact descendants are provider-owned.
    ExternalRoot,
    /// A superseded identity retained for succession after release.
    Retired,
}

impl Role {
    pub fn word(&self) -> &'static str {
        match self {
            Role::Family => "family",
            Role::Leaf => "leaf",
            Role::FamilyAndLeaf => "family_leaf",
            Role::ExternalRoot => "external_root",
            Role::Retired => "retired",
        }
    }
}

/// One node of the declared tree. Every consumer of the hierarchy reads
/// these nodes and nothing else: the registry projects them, a selector is
/// admitted by them, a received identity is decoded against them.
struct Declared {
    hierarchy: Vec<&'static str>,
    role: Role,
    /// The class every occurrence at this node carries; `None` for
    /// families and external roots.
    class: Option<DiagnosticClass>,
    /// The emitted identity's descriptor: a leaf's own, a family-and-leaf's
    /// own leaf, an external root's descriptor. `None` for a pure family.
    leaf: Option<&'static LeafDescriptor>,
    external: Option<&'static ExternalTail>,
    summary: String,
    explanation: String,
}

impl Declared {
    fn emits(&self) -> bool {
        matches!(self.role, Role::Leaf | Role::FamilyAndLeaf)
    }
}

/// Every node the root declares, families and leaves alike, root first in
/// declaration order. A multi-segment leaf `a/b/c` declares the implicit
/// families `a` and `a/b` of its enclosing node; each implicit family is
/// listed once.
fn declared<T: Taxon>() -> Vec<Declared> {
    let mut nodes = Vec::new();
    walk(T::CHILDREN, T::EXTERNAL, &[], &mut nodes);
    nodes
}

fn join(prefix: &[&str], more: &[&str]) -> String {
    prefix
        .iter()
        .chain(more.iter())
        .copied()
        .collect::<Vec<_>>()
        .join("/")
}

fn walk(
    children: &[Node],
    external: Option<&'static ExternalTail>,
    prefix: &[&'static str],
    nodes: &mut Vec<Declared>,
) {
    // The family's own emitted identity, if a child leaf has no segments.
    let own_leaf = children.iter().find_map(|n| match n {
        Node::Leaf(l) if l.segments.is_empty() => Some(*l),
        _ => None,
    });
    if !prefix.is_empty() {
        // The node for `prefix` itself was pushed by the parent as a family;
        // settle its role now that its children are known.
        let me = nodes.last_mut().expect("the parent pushed this family");
        debug_assert_eq!(me.hierarchy, prefix);
        if let Some(leaf) = own_leaf {
            me.role = Role::FamilyAndLeaf;
            me.class = leaf.class;
            me.leaf = Some(leaf);
        } else if let Some(tail) = external {
            me.role = Role::ExternalRoot;
            me.leaf = Some(tail.descriptor);
            me.external = Some(tail);
        }
    }
    // Implicit intermediate families: a leaf `a/b/c` declares `a` and `a/b`
    // as families of this node without an enum of their own. Each is listed
    // once, with every leaf under it as its members.
    let mut implicit: Vec<(Vec<&'static str>, Vec<&'static LeafDescriptor>)> = Vec::new();
    for child in children {
        match child {
            Node::Family(f) => {
                let mut path: Vec<&'static str> = prefix.to_vec();
                path.push(f.segment);
                nodes.push(Declared {
                    hierarchy: path.clone(),
                    role: Role::Family,
                    class: None,
                    leaf: None,
                    external: None,
                    summary: f.summary.to_string(),
                    explanation: f.explanation.to_string(),
                });
                walk(f.children, f.external, &path, nodes);
            }
            Node::Leaf(l) => {
                if l.segments.is_empty() {
                    continue;
                }
                for depth in 1..l.segments.len() {
                    let mut path: Vec<&'static str> = prefix.to_vec();
                    path.extend_from_slice(&l.segments[..depth]);
                    match implicit.iter_mut().find(|(h, _)| *h == path) {
                        Some((_, leaves)) => leaves.push(l),
                        None => implicit.push((path, vec![l])),
                    }
                }
                let mut path: Vec<&'static str> = prefix.to_vec();
                path.extend_from_slice(l.segments);
                nodes.push(Declared {
                    hierarchy: path,
                    role: Role::Leaf,
                    class: l.class,
                    leaf: Some(l),
                    external: None,
                    summary: l.summary.to_string(),
                    explanation: l.explanation.to_string(),
                });
            }
        }
    }
    for (path, leaves) in implicit {
        // An explicit family of the same name would be a second declaration
        // of one path; the reconciliation test refuses the duplicate.
        let spelled = path.join("/");
        let members = leaves
            .iter()
            .map(|l| join(prefix, l.segments))
            .collect::<Vec<_>>()
            .join(", ");
        nodes.push(Declared {
            hierarchy: path,
            role: Role::Family,
            class: None,
            leaf: None,
            external: None,
            summary: format!("The {spelled} family."),
            explanation: format!(
                "A family node: a hook naming it matches every identity under it. \
                 Members: {members}."
            ),
        });
    }
}

/// One row of the registry projection.
#[derive(Debug, Clone)]
pub struct InventoryRow {
    pub hierarchy: String,
    pub role: Role,
    /// The class every occurrence at this hierarchy carries; `None` for
    /// families, external roots, and retired rows.
    pub class: Option<DiagnosticClass>,
    pub summary: String,
    pub explanation: String,
}

/// Every hierarchy the root declares, families and leaves alike, root first
/// in declaration order — plus the retired population.
pub fn inventory<T: Taxon>() -> Vec<InventoryRow> {
    let mut rows: Vec<InventoryRow> = declared::<T>()
        .into_iter()
        .map(|node| InventoryRow {
            hierarchy: node.hierarchy.join("/"),
            role: node.role,
            class: node.class,
            summary: node.summary,
            explanation: node.explanation,
        })
        .collect();
    for (retired, successor) in RETIRED {
        rows.push(InventoryRow {
            hierarchy: (*retired).to_string(),
            role: Role::Retired,
            class: None,
            summary: format!("Retired; superseded by {successor}."),
            explanation: format!(
                "This identity is no longer emitted. Current production emits {successor}; \
                 a selector naming the retired path matches its successor."
            ),
        });
    }
    rows
}

/// Superseded identities retained for succession, `(retired, successor)`.
/// Append-only from the first public release; empty before it, because a
/// pre-release identity that appeared in no released version is deleted
/// rather than retired. This table is the one succession law: nothing
/// outside this module supplies another.
pub const RETIRED: &[(&str, &str)] = &[];

/// The successor a retired path names, under one retirement table.
fn successor<'a>(retired: &'a [(&'a str, &'a str)], path: &str) -> Option<&'a str> {
    retired
        .iter()
        .find(|(name, _)| *name == path)
        .map(|(_, successor)| *successor)
}

/// How a path stands against the declared tree.
enum Standing<'a> {
    /// Exactly a declared node.
    Node(&'a Declared),
    /// Under an external root, with a tail the provider admits.
    ExternalTail(&'a Declared, Vec<String>),
    Absent,
}

fn standing<'a>(nodes: &'a [Declared], segments: &[&str]) -> Standing<'a> {
    if let Some(node) = nodes.iter().find(|n| n.hierarchy == segments) {
        return Standing::Node(node);
    }
    // The deepest external root the path stands under.
    let root = nodes
        .iter()
        .filter(|n| n.role == Role::ExternalRoot)
        .filter(|n| {
            n.hierarchy.len() < segments.len() && segments[..n.hierarchy.len()] == n.hierarchy[..]
        })
        .max_by_key(|n| n.hierarchy.len());
    match root {
        Some(node) => {
            let tail = &segments[node.hierarchy.len()..];
            let external = node.external.expect("an external root carries its tail");
            if (external.validate)(tail) {
                Standing::ExternalTail(node, tail.iter().map(|s| s.to_string()).collect())
            } else {
                Standing::Absent
            }
        }
        None => Standing::Absent,
    }
}

// ----------------------------------------------------------------------------
// Received identities: a peer's typed occurrence, kept typed
// ----------------------------------------------------------------------------

/// An occurrence RECEIVED over the protocol whose identity the declared
/// tree answers for: a peer party of this build said what it refused, and
/// this process carries that statement on as the same typed fact — never as
/// prose to be re-badged. Identity, class and descriptor are the tree's;
/// only the message is the peer's.
#[derive(Clone)]
pub struct Received {
    id: ErrorId,
    class: DiagnosticClass,
    leaf: &'static LeafDescriptor,
    message: String,
}

impl fmt::Debug for Received {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Received")
            .field("id", &self.id)
            .field("class", &self.class)
            .field("message", &self.message)
            .finish()
    }
}

impl PartialEq for Received {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.class == other.class && self.message == other.message
    }
}

impl Eq for Received {}

impl Received {
    /// Decode a wire identity against the declared tree of `T`. `None` when
    /// the bytes are not a badge URI or name nothing the tree emits — a
    /// family, a foreign identity, an unvalidated tail — so that the caller
    /// judges the protocol, not the identity.
    pub fn decode<T: Taxon>(identity: &[u8], message: &[u8]) -> Option<Received> {
        let text = String::from_utf8_lossy(identity);
        let hierarchy = text.strip_prefix(ERROR_URI_SCHEME)?;
        let segments: Vec<&str> = hierarchy.split('/').collect();
        if segments.iter().any(|s| s.is_empty()) {
            return None;
        }
        let nodes = declared::<T>();
        let message = String::from_utf8_lossy(message).into_owned();
        match standing(&nodes, &segments) {
            Standing::Node(node) if node.emits() => Some(Received {
                id: ErrorId::declared(node.hierarchy.clone(), Vec::new()),
                class: node.class?,
                leaf: node.leaf?,
                message,
            }),
            Standing::ExternalTail(node, tail) => {
                let external = node.external?;
                let borrowed: Vec<&str> = tail.iter().map(String::as_str).collect();
                Some(Received {
                    id: ErrorId::declared(node.hierarchy.clone(), tail.clone()),
                    class: (external.class_of_tail)(&borrowed)?,
                    leaf: external.descriptor,
                    message,
                })
            }
            _ => None,
        }
    }

    pub fn id(&self) -> &ErrorId {
        &self.id
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Received {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Received {}

impl Carried for Received {
    fn push_segments(&self, out: &mut Vec<&'static str>) {
        out.extend_from_slice(self.id.fixed_segments());
    }
    fn tail(&self) -> Vec<String> {
        self.id.tail.clone()
    }
    fn leaf(&self) -> &'static LeafDescriptor {
        self.leaf
    }
    fn class(&self) -> DiagnosticClass {
        self.class
    }
}

// ----------------------------------------------------------------------------
// Selectors: what an authored expectation denotes
// ----------------------------------------------------------------------------

/// What an authored `(~~error://… ~~)` expectation denotes: any error, a
/// family, an exact leaf, a retired alias, or a validated provider tail.
/// Parsed once, against the declared tree; matched against typed identities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorSelector {
    /// Empty means any error.
    segments: Vec<String>,
}

/// Why an authored path is not a selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectorRefusal {
    /// A DelightQL-owned path no family, leaf, retired alias, or validated
    /// external tail answers to: a typo.
    Unknown { path: String },
}

impl fmt::Display for SelectorRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectorRefusal::Unknown { path } => write!(
                f,
                "'{path}' is not a DelightQL error family or identity; \
                 `dql explain delightql-error://{path}` knows nothing of it"
            ),
        }
    }
}

/// Admission under one declared tree and one retirement table.
fn admit(
    nodes: &[Declared],
    retired: &[(&str, &str)],
    segments: &[&str],
) -> Result<ErrorSelector, SelectorRefusal> {
    if segments.is_empty() {
        return Ok(ErrorSelector::any());
    }
    let path = segments.join("/");
    let lawful = successor(retired, &path).is_some()
        || !matches!(standing(nodes, segments), Standing::Absent);
    if lawful {
        Ok(ErrorSelector {
            segments: segments.iter().map(|s| s.to_string()).collect(),
        })
    } else {
        Err(SelectorRefusal::Unknown { path })
    }
}

/// Containment under one retirement table: the identity is the selected
/// node (or its successor) or stands under it.
fn contains(retired: &[(&str, &str)], selected: &[String], actual: &[&str]) -> bool {
    if selected.is_empty() {
        return true;
    }
    let path = selected.join("/");
    let selected: Vec<&str> = match successor(retired, &path) {
        Some(successor) => successor.split('/').collect(),
        None => selected.iter().map(String::as_str).collect(),
    };
    actual.len() >= selected.len() && actual[..selected.len()] == selected[..]
}

impl ErrorSelector {
    /// The bare hook: any error at all.
    pub fn any() -> ErrorSelector {
        ErrorSelector {
            segments: Vec::new(),
        }
    }

    /// Validate an authored path against the declared tree and the one
    /// retirement table. An empty list is the bare hook.
    pub fn parse<T: Taxon>(segments: &[&str]) -> Result<ErrorSelector, SelectorRefusal> {
        admit(&declared::<T>(), RETIRED, segments)
    }

    /// Whether the identity is the selected node or stands under it.
    pub fn matches(&self, id: &ErrorId) -> bool {
        let actual: Vec<&str> = id.segments().collect();
        contains(RETIRED, &self.segments, &actual)
    }

    /// The authored spelling, for verdict display.
    pub fn display(&self) -> String {
        if self.segments.is_empty() {
            "(any error)".to_string()
        } else {
            format!("error://{}", self.segments.join("/"))
        }
    }

    pub fn is_any(&self) -> bool {
        self.segments.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostic::{Authored, DelightQLError, Resolution};

    fn nodes() -> Vec<Declared> {
        declared::<DelightQLError>()
    }

    /// Succession is decided by one table. The production table is empty
    /// before release; the mechanism is proved on a fixture that nothing
    /// outside this module can supply.
    #[test]
    fn a_retired_alias_selects_its_successor() {
        let retired = [("runtime/assertion", "authored/abort")];
        let abort: DelightQLError = Authored::Abort {
            label: String::new(),
            observation_failure: None,
        }
        .into();
        let table: DelightQLError = Resolution::Table {
            table: "t".to_string(),
            context: String::new(),
        }
        .into();
        let alias = admit(&nodes(), &retired, &["runtime", "assertion"])
            .expect("a retired alias is a lawful selector");
        let under = |id: &ErrorId| {
            let actual: Vec<&str> = id.segments().collect();
            contains(&retired, &alias.segments, &actual)
        };
        assert!(under(&abort.id()));
        assert!(!under(&table.id()));
        assert!(
            admit(&nodes(), RETIRED, &["runtime", "assertion"]).is_err(),
            "without the table the alias is a typo"
        );
        assert!(
            RETIRED.is_empty(),
            "before the first public release nothing is retired; delete instead"
        );
    }

    /// Every node a selector may name is an inventory row, and every prefix
    /// of every declared node is itself a declared node: the tree is
    /// prefix-closed, so admission and projection cannot disagree.
    #[test]
    fn the_declared_tree_is_prefix_closed() {
        let nodes = nodes();
        for node in &nodes {
            for depth in 1..node.hierarchy.len() {
                let prefix = &node.hierarchy[..depth];
                assert!(
                    nodes.iter().any(|n| n.hierarchy == prefix),
                    "{} has no declared node for its prefix {}",
                    node.hierarchy.join("/"),
                    prefix.join("/")
                );
            }
            let borrowed: Vec<&str> = node.hierarchy.to_vec();
            assert!(
                matches!(standing(&nodes, &borrowed), Standing::Node(_)),
                "{} is not admitted",
                node.hierarchy.join("/")
            );
        }
    }
}
