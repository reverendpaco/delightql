// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Authored names, decoded once.
//!
//! Stropping is SPELLING (strops-law): a strop is a reference, never a value,
//! and the engine-facing bytes are never stripped. Which of the two spellings
//! an identifier used is a TYPED question here — `identifier` has a
//! `stropped_form` child or it does not — so no reader hunts for backticks.
//!
//! Every reference the compiler carries is minted at this one boundary. The
//! `Ref` it produces owns its canonical spelling, its namespace, its mark
//! (`!` is effect identity, a trailing `::` is the catalog functor) and its
//! resolution routing; no node downstream receives a split namespace to
//! reassemble.

use super::Normalizer;
use crate::diagnostic::{Internal, Resolution};
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::metadata::NamespacePath;
use crate::pipeline::asts::core::QualifiedName;
use crate::pipeline::asts::vocabulary::{
    Mark, Namespace, QualifierRoute, Ref, ResolutionMode, Vec1,
};
use crate::pipeline::syntax::cst;
use delightql_types::SqlIdentifier;

impl<'t> Normalizer<'t> {
    /// An authored identifier, keeping its stroppedness. A stropped name is
    /// case-sensitive; a classic one is not, and that contract is decided
    /// here rather than by anyone re-reading the characters.
    pub(crate) fn identifier(&self, node: cst::Identifier<'t>) -> SqlIdentifier {
        identifier_of(self.tree, node)
    }

    /// A name OFFERED as a binder: a caller-pattern slot's, a tree pattern
    /// member's. A binder is a bare name by grammar, so there is no
    /// qualifier to carry and no reader that could look for one.
    pub(crate) fn written_binder(
        &self,
        node: cst::Identifier<'t>,
    ) -> crate::pipeline::asts::core::WrittenBinder {
        crate::pipeline::asts::core::WrittenBinder {
            name: self.identifier(node),
            namespace_path: crate::pipeline::asts::core::NamespacePath::empty(),
        }
    }

    /// THE SLASH RIDES THE NAME. The token spans `/name` so that longest-match
    /// can tell an engine reference from division — `x/2.2` is division,
    /// because `2.2` is no identifier — which leaves the slash here to drop.
    pub(crate) fn engine_name(&self, node: cst::EngineName<'t>) -> SqlIdentifier {
        let text = self.text(node).trim_start_matches('/');
        match text.starts_with('`') {
            true => SqlIdentifier::stropped(strop_interior(text)),
            false => SqlIdentifier::new(text),
        }
    }

    /// A namespace's segments, outermost first — `lib::math` is
    /// `[lib, math]`.
    /// The one CST-boundary decode of a written namespace: its route (exact,
    /// or the self-relative `.::child`) and its segments in path order.
    pub(crate) fn namespace_parts(
        &self,
        node: cst::Namespace<'t>,
    ) -> (QualifierRoute, Vec<SqlIdentifier>) {
        let mut route = QualifierRoute::Exact;
        let mut segments = Vec::new();
        for child in node.children() {
            match child {
                cst::NamespaceChild::SelfRelative(_) => route = QualifierRoute::SelfRelative,
                cst::NamespaceChild::Identifier(part) => segments.push(self.identifier(part)),
            }
        }
        (route, segments)
    }

    fn namespace_of(&self, qual: Option<cst::NamespaceQual<'t>>) -> Result<Namespace> {
        let Some(qual) = qual else {
            // "Nothing written" is a VALUE, not a hole.
            return Ok(Namespace::Ambient);
        };
        let path = self.require(qual.child(), "a namespace qualifier has a namespace")?;
        let (route, segments) = self.namespace_parts(path);
        let parts: Vec<_> = segments
            .into_iter()
            .map(|segment| {
                self.registry
                    .intern(segment.as_str(), segment.is_stropped())
            })
            .collect();
        let parts = self.require(
            Vec1::try_from_vec(parts),
            "a namespace has at least one segment",
        )?;
        Ok(match route {
            QualifierRoute::Exact => Namespace::Path(parts),
            QualifierRoute::SelfRelative => Namespace::SelfRelative(parts),
        })
    }

    /// The one CST-boundary decode of a written reference.
    pub(crate) fn reference(
        &self,
        node: cst::PredicateIdentifier<'t>,
        mark: Mark,
        resolution: ResolutionMode,
    ) -> Result<Ref> {
        let name = self.require(node.name(), "a predicate identifier has a name")?;
        self.written_reference(node.namespace(), name, mark, resolution)
    }

    /// A written reference from its two parts. The citation spells them
    /// around its mark (`ns.:f`) rather than as one `predicate_identifier`,
    /// and decodes through the same act.
    pub(crate) fn written_reference(
        &self,
        namespace: Option<cst::NamespaceQual<'t>>,
        name: cst::Identifier<'t>,
        mark: Mark,
        resolution: ResolutionMode,
    ) -> Result<Ref> {
        let name = self.identifier(name);
        Ok(Ref::written(
            std::rc::Rc::clone(&self.registry),
            self.namespace_of(namespace)?,
            self.registry.intern(name.as_str(), name.is_stropped()),
            mark,
            resolution,
        ))
    }

    /// A plain pure reference — the common case.
    pub(crate) fn plain_reference(&self, node: cst::PredicateIdentifier<'t>) -> Result<Ref> {
        self.reference(node, Mark::Plain, ResolutionMode::Normal)
    }

    /// The `!` is part of the NAME: `stdout!` IS the entity, not `stdout`
    /// wearing a flag.
    pub(crate) fn effect_reference(&self, node: cst::EffectIdentifier<'t>) -> Result<Ref> {
        for child in node.children() {
            if let cst::EffectIdentifierChild::PredicateIdentifier(inner) = child {
                let name = self.require(inner.name(), "a predicate identifier has a name")?;
                let name = self.identifier(name);
                // `stdout!` IS the name. The marker is part of the interned
                // spelling, not a flag beside it, so a reader that has only
                // the reference still knows what it names.
                let marked = format!("{}!", name.as_str());
                return Ok(Ref::written(
                    std::rc::Rc::clone(&self.registry),
                    self.namespace_of(inner.namespace())?,
                    self.registry.intern(&marked, name.is_stropped()),
                    Mark::Effect,
                    ResolutionMode::Normal,
                ));
            }
        }
        Err(Internal::invariant(
            "normalize::names",
            "an effect identifier has a predicate identifier",
        ))
    }

    /// THE ENGINE'S CATALOG IS THE ENGINE'S — the slash routes the lookup
    /// past DQL's catalog into the target engine's own namespace. The
    /// engine segment is the namespace and the routing is the mode; nothing
    /// downstream re-reads a slash.
    pub(crate) fn engine_reference(&self, node: cst::EngineReference<'t>) -> Result<Ref> {
        let engine = self.require(node.engine(), "an engine reference names its engine")?;
        let name = self.require(node.name(), "an engine reference names its relation")?;
        let engine = self.identifier(engine);
        let name = self.engine_name(name);
        Ok(Ref::written(
            std::rc::Rc::clone(&self.registry),
            Namespace::Path(Vec1::new(
                self.registry.intern(engine.as_str(), engine.is_stropped()),
            )),
            self.registry.intern(name.as_str(), name.is_stropped()),
            Mark::Plain,
            ResolutionMode::TargetPassthrough,
        ))
    }

    /// A relation name in either of its two spellings.
    pub(crate) fn relation_reference(&self, node: cst::RelationName<'t>) -> Result<Ref> {
        match self.require(node.child(), "a relation name has a spelling")? {
            cst::RelationNameChild::PredicateIdentifier(name) => self.plain_reference(name),
            cst::RelationNameChild::EngineReference(name) => self.engine_reference(name),
        }
    }

    /// The spelling-shaped name the relational carriers still address by
    /// characters. The namespace path is stored the way `AuthoredColumn`
    /// stores one: innermost first.
    pub(crate) fn qualified_name(
        &self,
        node: cst::PredicateIdentifier<'t>,
    ) -> Result<QualifiedName> {
        let name = self.require(node.name(), "a predicate identifier has a name")?;
        Ok(QualifiedName {
            namespace_path: self.namespace_path(node.namespace())?,
            name: self.identifier(name),
        })
    }

    /// A relation-position predicate name whose leaf has passed authored
    /// identifier admission before any definition, catalog or target lookup.
    pub(crate) fn qualified_reference_name(
        &self,
        node: cst::PredicateIdentifier<'t>,
    ) -> Result<QualifiedName> {
        let name = self.require(node.name(), "a predicate identifier has a name")?;
        let name = self.admit_reference(self.identifier(name))?;
        let namespace_path = match node.namespace() {
            None => NamespacePath::empty(),
            Some(qual) => {
                let path = self.require(qual.child(), "a namespace qualifier has a namespace")?;
                let (route, segments) = self.namespace_parts(path);
                let parts = segments
                    .into_iter()
                    .map(|segment| self.admit_reference(segment))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .map(|segment| segment.as_str().to_string())
                    .collect();
                routed_path(route, parts)?
            }
        };
        Ok(QualifiedName {
            namespace_path,
            name,
        })
    }

    pub(crate) fn namespace_path(
        &self,
        qual: Option<cst::NamespaceQual<'t>>,
    ) -> Result<NamespacePath> {
        let Some(qual) = qual else {
            return Ok(NamespacePath::empty());
        };
        let path = self.require(qual.child(), "a namespace qualifier has a namespace")?;
        let (route, segments) = self.namespace_parts(path);
        routed_path(
            route,
            segments
                .into_iter()
                .map(|segment| segment.as_str().to_string())
                .collect(),
        )
    }

    /// A qualifier in reference position. The deictic `_` names a RELATION —
    /// the unnamed stage — and disregards nothing; position is what tells it
    /// apart from the anaphor, which is why the two arrive as different CST
    /// members and not as one glyph to be classified.
    pub(crate) fn qualifier(&self, node: cst::Qualifier<'t>) -> Result<Qualified> {
        match node {
            cst::Qualifier::QualifierName(name) => {
                let inner = self.require(name.children().next(), "a qualifier names something")?;
                Ok(Qualified::Named(self.identifier(inner)))
            }
            cst::Qualifier::DeicticStage(_) => Ok(Qualified::DeicticStage),
        }
    }
}

/// What a written qualifier addresses.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Qualified {
    /// A relation, alias, or stage spelled by name.
    Named(SqlIdentifier),
    /// `_` — the one unnamed pipe output in scope.
    DeicticStage,
}

impl Qualified {
    /// The spelling the unresolved tree carries for this qualifier.
    ///
    /// The deictic stage has no name, and the unresolved carriers address
    /// qualifiers by characters, so it travels as the glyph the resolver
    /// already answers for. When resolution learns to take the reference
    /// itself this conversion is the one place that changes.
    pub(crate) fn spelling(&self) -> SqlIdentifier {
        match self {
            Qualified::Named(name) => name.clone(),
            Qualified::DeicticStage => SqlIdentifier::new("_"),
        }
    }
}

/// A written path under its route: the exact path, or the self-relative
/// child route the world resolves.
fn routed_path(route: QualifierRoute, parts: Vec<String>) -> Result<NamespacePath> {
    match route {
        QualifierRoute::Exact => NamespacePath::from_parts(parts),
        QualifierRoute::SelfRelative => NamespacePath::self_relative(parts),
    }
    .map_err(|error| {
        crate::diagnostic::DelightQLError::from(crate::diagnostic::Parse::General {
            message: format!("invalid namespace path: {error:?}"),
        })
    })
}

/// An authored identifier, keeping its stroppedness — for a reader holding
/// the tree rather than the normalizer.
///
/// ONE place decides what stropping means. A second reader that hunted for
/// backticks would be a second answer to a typed question, and the two would
/// disagree the first time a name arrived by an unexpected road.
pub(crate) fn identifier_of(
    tree: &crate::pipeline::syntax::SyntaxTree,
    node: cst::Identifier<'_>,
) -> SqlIdentifier {
    match node.child() {
        Some(stropped) => SqlIdentifier::stropped(strop_interior(tree.text(stropped))),
        None => SqlIdentifier::new(tree.text(node)),
    }
}

/// The characters inside a strop. The delimiters are not part of the name;
/// everything between them is, byte for byte.
fn strop_interior(text: &str) -> &str {
    text.strip_prefix('`')
        .and_then(|rest| rest.strip_suffix('`'))
        .unwrap_or(text)
}

/// A DEFINITION-OWNED SCALAR REFERENCE, `$.x`: selected here, in the marked
/// scopes this text is read under, nearest first. What leaves is the
/// selection — never the spelling — so no later stage can bind the name to
/// anything else.
impl<'t> Normalizer<'t> {
    pub(crate) fn parameter_reference(
        &self,
        node: cst::ParameterReference<'t>,
    ) -> Result<crate::pipeline::asts::core::definitions::FormalSelector> {
        let name = match self.require(node.name(), "a parameter reference names its formal")? {
            cst::ParameterReferenceName::Identifier(name) => self.identifier(name),
            cst::ParameterReferenceName::StroppedForm(name) => {
                SqlIdentifier::stropped(strop_interior(self.text(name)))
            }
        };
        self.marked.select(&name).ok_or_else(|| {
            DelightQLError::from(Resolution::Parameter {
                message: format!(
                    "'$.{name}' names no scalar formal of a higher-order definition it stands \
                     in. `$.x` reads a formal declared by the relational or effect \
                     higher-order clause the reference is written in, or by one enclosing \
                     it; a column is written bare, and a value function's or lambda's \
                     parameter is written bare too"
                ),
            })
        })
    }

    /// A compile-time integer position (FN.40): the number written, or the
    /// scalar formal written there, recorded by the selection its `$.x`
    /// makes. What a formal stands for at a use is not decided here.
    pub(crate) fn compile_time_integer(
        &mut self,
        node: cst::CompileTimeInteger<'t>,
        position: &'static str,
    ) -> Result<crate::pipeline::asts::core::CompileTimeInteger> {
        use crate::pipeline::asts::core::CompileTimeInteger;
        match node {
            cst::CompileTimeInteger::Number(number) => {
                let text = self.text(number);
                text.parse::<i64>().map(CompileTimeInteger::Number).map_err(|_| {
                    Internal::invariant(
                        "normalize::names",
                        format!("{position} takes a whole number; '{text}' is not one"),
                    )
                })
            }
            cst::CompileTimeInteger::ParameterReference(parameter) => {
                self.parameter_reference(parameter).map(CompileTimeInteger::Formal)
            }
        }
    }
}
