// The typed-CST generator.
//
// Reads the `node-types.json` Tree-sitter writes beside the parser and emits a
// Rust API into `OUT_DIR`. Nothing it produces is a source file: the grammar is
// the authority, the generated Rust is derived, and the only checked-in Rust is
// the façade that re-exports it.
//
// Two shapes come out:
//
//   - a zero-copy struct per concrete node kind, with one accessor per field
//     and one for its unfielded named children;
//   - an enum per SUPERTYPE, whose variants are exactly that supertype's
//     members.
//
// The enums are the point. A consumer matching on `Continuation` cannot
// silently miss a member, so adding an alternative to the grammar becomes a
// compile error in every consumer rather than a branch that quietly does
// nothing — which is the failure mode a raw `node.kind()` string comparison
// has no way to prevent.

use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

const RUST_KEYWORDS: &[&str] = &[
    "as", "async", "await", "become", "box", "break", "const", "continue", "crate", "do", "dyn",
    "else", "enum", "extern", "false", "final", "fn", "for", "if", "impl", "in", "let", "loop",
    "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref", "return", "self",
    "static", "struct", "super", "trait", "true", "try", "type", "typeof", "unsafe", "unsized",
    "use", "virtual", "where", "while", "yield",
];

/// Accessor and variant names collide with the inherent API every wrapper has;
/// a field literally named `node` would shadow the escape hatch.
const RESERVED_METHODS: &[&str] = &[
    "node",
    "kind",
    "from_node",
    "byte_range",
    "text",
    "children",
];

fn is_plain_ident(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Names the Rust prelude already owns. A node kind that pascal-cases into one
/// of these would shadow it for every consumer doing `use cst::*` — `string`
/// becoming `String` silently breaks `format!`. Those get a suffix; nothing
/// else does, because the whole value of the typed API is that a node reads
/// under the grammar's own name.
const PRELUDE_NAMES: &[&str] = &[
    "Box",
    "Clone",
    "Copy",
    "Default",
    "Drop",
    "Err",
    "Fn",
    "FnMut",
    "FnOnce",
    "From",
    "Into",
    "Iterator",
    "None",
    "Ok",
    "Option",
    "Ord",
    "PartialOrd",
    "Result",
    "Send",
    "Sized",
    "Some",
    "String",
    "Sync",
    "ToString",
    "Vec",
];

fn pascal(s: &str) -> String {
    let name: String = s
        .split('_')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut c = p.chars();
            match c.next() {
                Some(f) => f.to_ascii_uppercase().to_string() + c.as_str(),
                None => String::new(),
            }
        })
        .collect();
    if PRELUDE_NAMES.contains(&name.as_str()) {
        format!("{name}Node")
    } else {
        name
    }
}

fn method_name(s: &str) -> String {
    if RESERVED_METHODS.contains(&s) {
        // A raw identifier cannot rescue this one: the name is taken by the
        // wrapper's own API, so the field accessor is suffixed instead.
        format!("{s}_field")
    } else if RUST_KEYWORDS.contains(&s) {
        format!("r#{s}")
    } else {
        s.to_string()
    }
}

struct NodeType {
    kind: String,
    subtypes: Vec<String>,
    fields: BTreeMap<String, ChildSet>,
    children: Option<ChildSet>,
}

struct ChildSet {
    multiple: bool,
    required: bool,
    types: Vec<String>,
}

fn child_set(v: &Value) -> ChildSet {
    let types = v
        .get("types")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter(|t| t.get("named").and_then(Value::as_bool).unwrap_or(false))
                .filter_map(|t| t.get("type").and_then(Value::as_str))
                .filter(|t| is_plain_ident(t))
                .map(str::to_string)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect()
        })
        .unwrap_or_default();
    ChildSet {
        multiple: v.get("multiple").and_then(Value::as_bool).unwrap_or(false),
        required: v.get("required").and_then(Value::as_bool).unwrap_or(false),
        types,
    }
}

pub fn generate(node_types_json: &str) -> String {
    let parsed: Vec<Value> = serde_json::from_str(node_types_json).expect("node-types.json");

    let mut nodes: Vec<NodeType> = Vec::new();
    for entry in &parsed {
        if !entry.get("named").and_then(Value::as_bool).unwrap_or(false) {
            continue;
        }
        let Some(kind) = entry.get("type").and_then(Value::as_str) else {
            continue;
        };
        if !is_plain_ident(kind) {
            continue;
        }
        let subtypes = entry
            .get("subtypes")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|t| t.get("type").and_then(Value::as_str))
                    .filter(|t| is_plain_ident(t))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let fields = entry
            .get("fields")
            .and_then(Value::as_object)
            .map(|o| {
                o.iter()
                    .filter(|(k, _)| is_plain_ident(k))
                    .map(|(k, v)| (k.clone(), child_set(v)))
                    .collect()
            })
            .unwrap_or_default();
        let children = entry.get("children").map(child_set);
        nodes.push(NodeType {
            kind: kind.to_string(),
            subtypes,
            fields,
            children,
        });
    }

    // THE EXTRAS ARE THE GRAMMAR'S, NOT A READER'S. Tree-sitter marks every
    // member of `grammar.js`'s `extras` list here, so a consumer that must
    // step over them reads this set instead of keeping its own spellings —
    // and adding an extra to the grammar reaches that consumer without
    // anyone remembering to tell it.
    let extras: Vec<&str> = parsed
        .iter()
        .filter(|entry| entry.get("extra").and_then(Value::as_bool).unwrap_or(false))
        .filter_map(|entry| entry.get("type").and_then(Value::as_str))
        .collect();

    let mut out = String::new();
    out.push_str(HEADER);

    writeln!(
        out,
        "/// Every node kind the grammar declares EXTRA — admitted between any\n\
         /// two tokens and contributing no structure. Whitespace is an extra\n\
         /// too but produces no node, so it never appears here or in a token\n\
         /// stream."
    )
    .unwrap();
    writeln!(out, "pub const EXTRA_KINDS: &[&str] = &[").unwrap();
    for kind in &extras {
        writeln!(out, "    {kind:?},").unwrap();
    }
    writeln!(out, "];\n").unwrap();

    // The supertype membership table, as data. A test can diff two member sets
    // mechanically — which is what MIRROR LAW asks for — without re-deriving
    // them by hand and without reading a derived file off disk.
    writeln!(
        out,
        "/// Every supertype and its members, in the grammar's own order."
    )
    .unwrap();
    writeln!(out, "pub const SUBTYPES: &[(&str, &[&str])] = &[").unwrap();
    for n in nodes.iter().filter(|n| !n.subtypes.is_empty()) {
        let members: Vec<String> = n.subtypes.iter().map(|s| format!("{s:?}")).collect();
        writeln!(out, "    ({:?}, &[{}]),", n.kind, members.join(", ")).unwrap();
    }
    writeln!(out, "];\n").unwrap();
    writeln!(
        out,
        "/// The members of one supertype, or an empty slice if the name is not one."
    )
    .unwrap();
    writeln!(
        out,
        "pub fn subtypes_of(supertype: &str) -> &'static [&'static str] {{"
    )
    .unwrap();
    writeln!(
        out,
        "    SUBTYPES.iter().find(|(k, _)| *k == supertype).map(|(_, v)| *v).unwrap_or(&[])"
    )
    .unwrap();
    writeln!(out, "}}\n").unwrap();

    // One `Kind` over every named node the language can produce. Exhaustive by
    // construction, so a consumer can match the whole alphabet rather than
    // compare strings and hope.
    writeln!(
        out,
        "/// Every named node kind the consolidated grammar produces."
    )
    .unwrap();
    writeln!(out, "#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]").unwrap();
    writeln!(out, "#[non_exhaustive]").unwrap();
    writeln!(out, "pub enum Kind {{").unwrap();
    for n in &nodes {
        writeln!(out, "    {},", pascal(&n.kind)).unwrap();
    }
    writeln!(out, "}}\n").unwrap();
    writeln!(out, "impl Kind {{").unwrap();
    writeln!(out, "    pub fn from_str(s: &str) -> Option<Self> {{").unwrap();
    writeln!(out, "        Some(match s {{").unwrap();
    for n in &nodes {
        writeln!(
            out,
            "            {:?} => Kind::{},",
            n.kind,
            pascal(&n.kind)
        )
        .unwrap();
    }
    writeln!(out, "            _ => return None,").unwrap();
    writeln!(out, "        }})\n    }}\n").unwrap();
    writeln!(out, "    pub fn as_str(self) -> &'static str {{").unwrap();
    writeln!(out, "        match self {{").unwrap();
    for n in &nodes {
        writeln!(
            out,
            "            Kind::{} => {:?},",
            pascal(&n.kind),
            n.kind
        )
        .unwrap();
    }
    writeln!(out, "        }}\n    }}\n}}\n").unwrap();

    // The alphabet as DATA. `Kind` is `#[non_exhaustive]`, so a consumer
    // partitioning the language — a formatter deciding which kinds it lays out
    // and which it echoes — cannot get exhaustiveness from a `match`. It gets it
    // by diffing its own lists against this one, which the grammar writes.
    writeln!(
        out,
        "/// Every named kind, in the grammar's own order. The enumeration a\n\
         /// consumer partitioning the language must be complete against."
    )
    .unwrap();
    writeln!(out, "pub const ALL: &[Kind] = &[").unwrap();
    for n in &nodes {
        writeln!(out, "    Kind::{},", pascal(&n.kind)).unwrap();
    }
    writeln!(out, "];\n").unwrap();

    // Ad-hoc enums for fields and child slots admitting more than one kind.
    // Named after the node and slot they belong to, so two slots that happen to
    // admit the same set stay distinguishable at the call site.
    let mut adhoc: BTreeMap<String, Vec<String>> = BTreeMap::new();

    let slot_type = |owner: &str,
                     slot: &str,
                     cs: &ChildSet,
                     adhoc: &mut BTreeMap<String, Vec<String>>|
     -> Option<String> {
        match cs.types.len() {
            0 => None,
            1 => Some(format!("{}<'t>", pascal(&cs.types[0]))),
            _ => {
                let name = format!("{}{}", pascal(owner), pascal(slot));
                adhoc.insert(name.clone(), cs.types.clone());
                Some(format!("{name}<'t>"))
            }
        }
    };

    let mut body = String::new();
    for n in &nodes {
        let ty = pascal(&n.kind);
        if !n.subtypes.is_empty() {
            emit_enum(&mut body, &ty, &n.subtypes, Some(&n.kind));
            continue;
        }
        writeln!(body, "/// `{}`", n.kind).unwrap();
        writeln!(body, "#[derive(Clone, Copy)]").unwrap();
        writeln!(body, "pub struct {ty}<'t> {{ node: Node<'t> }}\n").unwrap();
        writeln!(body, "impl<'t> TypedNode<'t> for {ty}<'t> {{").unwrap();
        writeln!(body, "    const KIND: &'static str = {:?};", n.kind).unwrap();
        writeln!(
            body,
            "    fn cast(node: Node<'t>) -> Option<Self> {{ (node.kind() == Self::KIND).then_some({ty} {{ node }}) }}"
        )
        .unwrap();
        writeln!(body, "    fn node(&self) -> Node<'t> {{ self.node }}").unwrap();
        writeln!(body, "}}\n").unwrap();
        writeln!(body, "impl<'t> std::fmt::Debug for {ty}<'t> {{").unwrap();
        writeln!(
            body,
            "    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {{"
        )
        .unwrap();
        writeln!(
            body,
            "        f.debug_struct({:?}).field(\"range\", &self.node.byte_range()).finish()",
            n.kind
        )
        .unwrap();
        writeln!(body, "    }}\n}}\n").unwrap();

        writeln!(body, "impl<'t> {ty}<'t> {{").unwrap();
        for (fname, cs) in &n.fields {
            let Some(inner) = slot_type(&n.kind, fname, cs, &mut adhoc) else {
                continue;
            };
            let m = method_name(fname);
            if cs.multiple {
                writeln!(body, "    /// field `{fname}`").unwrap();
                writeln!(
                    body,
                    "    pub fn {m}(&self) -> impl Iterator<Item = {inner}> + 't {{ field_children(self.node, {fname:?}) }}"
                )
                .unwrap();
            } else if cs.required {
                writeln!(body, "    /// field `{fname}` (required by the grammar)").unwrap();
                writeln!(
                    body,
                    "    pub fn {m}(&self) -> Option<{inner}> {{ self.node.child_by_field_name({fname:?}).and_then(cast) }}"
                )
                .unwrap();
            } else {
                writeln!(body, "    /// field `{fname}`").unwrap();
                writeln!(
                    body,
                    "    pub fn {m}(&self) -> Option<{inner}> {{ self.node.child_by_field_name({fname:?}).and_then(cast) }}"
                )
                .unwrap();
            }
        }
        if let Some(cs) = &n.children {
            if let Some(inner) = slot_type(&n.kind, "child", cs, &mut adhoc) {
                writeln!(body, "    /// named children carrying no field name").unwrap();
                writeln!(
                    body,
                    "    pub fn children(&self) -> impl Iterator<Item = {inner}> + 't {{ unfielded_children(self.node) }}"
                )
                .unwrap();
                if !cs.multiple {
                    writeln!(body, "    /// the single unfielded named child").unwrap();
                    writeln!(
                        body,
                        "    pub fn child(&self) -> Option<{inner}> {{ self.children().next() }}"
                    )
                    .unwrap();
                }
            }
        }
        writeln!(body, "}}\n").unwrap();
    }

    for (name, types) in &adhoc {
        emit_enum(&mut out, name, types, None);
    }
    out.push_str(&body);

    // A parse tree can be traversed without knowing any kind at all: `AnyNode`
    // is the one untyped door, and it exists so that walking generic structure
    // never requires re-deriving the alphabet by hand.
    writeln!(out, "impl<'t> AnyNode<'t> {{").unwrap();
    writeln!(
        out,
        "    pub fn typed_kind(&self) -> Option<Kind> {{ Kind::from_str(self.node.kind()) }}"
    )
    .unwrap();
    writeln!(out, "}}").unwrap();

    out
}

fn emit_enum(out: &mut String, name: &str, members: &[String], kind: Option<&str>) {
    if let Some(k) = kind {
        writeln!(out, "/// supertype `{k}` — its members are exhaustive").unwrap();
    } else {
        writeln!(out, "/// the kinds one slot admits — exhaustive").unwrap();
    }
    writeln!(out, "#[derive(Clone, Copy, Debug)]").unwrap();
    writeln!(out, "pub enum {name}<'t> {{").unwrap();
    for m in members {
        writeln!(out, "    {}({}<'t>),", pascal(m), pascal(m)).unwrap();
    }
    writeln!(out, "}}\n").unwrap();

    writeln!(out, "impl<'t> TypedNode<'t> for {name}<'t> {{").unwrap();
    writeln!(
        out,
        "    const KIND: &'static str = {:?};",
        kind.unwrap_or(name)
    )
    .unwrap();
    writeln!(out, "    fn cast(node: Node<'t>) -> Option<Self> {{").unwrap();
    for m in members {
        writeln!(
            out,
            "        if let Some(n) = {}::cast(node) {{ return Some({name}::{}(n)); }}",
            pascal(m),
            pascal(m)
        )
        .unwrap();
    }
    writeln!(out, "        None\n    }}").unwrap();
    writeln!(out, "    fn node(&self) -> Node<'t> {{").unwrap();
    writeln!(out, "        match self {{").unwrap();
    for m in members {
        writeln!(out, "            {name}::{}(n) => n.node(),", pascal(m)).unwrap();
    }
    writeln!(out, "        }}\n    }}\n}}\n").unwrap();
}

const HEADER: &str = r#"// GENERATED from the consolidated grammar's node-types.json. Do not edit.

use tree_sitter::Node;

/// The one thing every typed node can do: name its kind, be built from a raw
/// node when the kind matches, and hand back the raw node.
///
/// Coordinates are deliberately NOT here. A host-selected root prepends a
/// selector the author never wrote, so a raw range is measured against text
/// that does not exist on disk. `SyntaxTree` owns the mapping and is the only
/// door to an authored span — a trait method taking a caller-supplied source
/// would be exactly how the wrong one gets used.
pub trait TypedNode<'t>: Sized + Copy {
    const KIND: &'static str;
    fn cast(node: Node<'t>) -> Option<Self>;
    fn node(&self) -> Node<'t>;

    /// The range in the text actually PARSED, selector included. Use
    /// `SyntaxTree::byte_range` for the range the author would recognise.
    fn raw_byte_range(&self) -> std::ops::Range<usize> { self.node().byte_range() }
    fn raw_start_position(&self) -> tree_sitter::Point { self.node().start_position() }
    fn raw_end_position(&self) -> tree_sitter::Point { self.node().end_position() }
}

fn cast<'t, T: TypedNode<'t>>(node: Node<'t>) -> Option<T> { T::cast(node) }

/// Any named node, typed only by position in the tree. The escape hatch for
/// generic traversal — never the road for a semantic decision.
#[derive(Clone, Copy, Debug)]
pub struct AnyNode<'t> { node: Node<'t> }

impl<'t> TypedNode<'t> for AnyNode<'t> {
    const KIND: &'static str = "";
    fn cast(node: Node<'t>) -> Option<Self> { node.is_named().then_some(AnyNode { node }) }
    fn node(&self) -> Node<'t> { self.node }
}

/// Children under one field name.
fn field_children<'t, T: TypedNode<'t>>(node: Node<'t>, field: &'static str) -> impl Iterator<Item = T> + 't {
    (0..node.child_count()).filter_map(move |i| {
        if node.field_name_for_child(i as u32) != Some(field) { return None; }
        node.child(i).and_then(T::cast)
    })
}

/// Named children carrying no field name.
fn unfielded_children<'t, T: TypedNode<'t>>(node: Node<'t>) -> impl Iterator<Item = T> + 't {
    (0..node.child_count()).filter_map(move |i| {
        if node.field_name_for_child(i as u32).is_some() { return None; }
        let c = node.child(i)?;
        if !c.is_named() { return None; }
        T::cast(c)
    })
}

"#;
