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
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::metadata::NamespacePath;
use crate::pipeline::asts::core::QualifiedName;
use crate::pipeline::asts::vocabulary::{Mark, Namespace, Ref, ResolutionMode, Vec1};
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
    pub(crate) fn namespace_segments(&self, node: cst::Namespace<'t>) -> Vec<SqlIdentifier> {
        node.children().map(|part| self.identifier(part)).collect()
    }

    fn namespace_of(&self, qual: Option<cst::NamespaceQual<'t>>) -> Result<Namespace> {
        let Some(qual) = qual else {
            // "Nothing written" is a VALUE, not a hole.
            return Ok(Namespace::Ambient);
        };
        let path = self.require(qual.child(), "a namespace qualifier has a namespace")?;
        let parts: Vec<_> = self
            .namespace_segments(path)
            .into_iter()
            .map(|segment| {
                self.registry
                    .intern(segment.as_str(), segment.is_stropped())
            })
            .collect();
        Ok(Namespace::Path(self.require(
            Vec1::try_from_vec(parts),
            "a namespace has at least one segment",
        )?))
    }

    /// The one CST-boundary decode of a written reference.
    pub(crate) fn reference(
        &self,
        node: cst::PredicateIdentifier<'t>,
        mark: Mark,
        resolution: ResolutionMode,
    ) -> Result<Ref> {
        let name = self.require(node.name(), "a predicate identifier has a name")?;
        let name = self.identifier(name);
        Ok(Ref::written(
            std::rc::Rc::clone(&self.registry),
            self.namespace_of(node.namespace())?,
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
        Err(DelightQLError::parse_error(
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

    pub(crate) fn namespace_path(
        &self,
        qual: Option<cst::NamespaceQual<'t>>,
    ) -> Result<NamespacePath> {
        let Some(qual) = qual else {
            return Ok(NamespacePath::empty());
        };
        let path = self.require(qual.child(), "a namespace qualifier has a namespace")?;
        NamespacePath::from_parts(
            self.namespace_segments(path)
                .into_iter()
                .map(|segment| segment.as_str().to_string())
                .collect(),
        )
        .map_err(|error| DelightQLError::parse_error(format!("invalid namespace path: {error:?}")))
    }

    /// A qualifier in reference position. The deictic `_` names a RELATION —
    /// the unnamed stage — and disregards nothing; position is what tells it
    /// apart from the anaphor, which is why the two arrive as different CST
    /// members and not as one glyph to be classified.
    pub(crate) fn qualifier(&self, node: cst::Qualifier<'t>) -> Result<Qualified> {
        match node {
            cst::Qualifier::QualifierName(name) => {
                let inner = self.require(name.children().next(), "a qualifier names something")?;
                Ok(Qualified::Named(
                    self.supplied_qualifier(self.identifier(inner)),
                ))
            }
            cst::Qualifier::DeicticStage(_) => Ok(Qualified::DeicticStage),
        }
    }

    /// A qualifier naming a relation FORMAL names what the call site supplied.
    /// `T(*)` and `T.id` address ONE relation, so the binding that swaps the
    /// read's spelling swaps the reference's; substituting only the read
    /// leaves the body qualifying a column by a name no longer in scope.
    ///
    /// The lookup is by BYTES. A formal `T` and an authored alias `t` are
    /// different qualifiers, and identifier folding would rewrite the alias
    /// into the supplied table.
    fn supplied_qualifier(&self, written: SqlIdentifier) -> SqlIdentifier {
        let Some(bindings) = self.bindings() else {
            return written;
        };
        let formal = written.as_str();
        // A compiler-owned carrier is addressed by IDENTITY and keeps the
        // authored formal its plan read carries; it has no table spelling,
        // and inventing one here would name nothing.
        if bindings.table_scope_params.contains_key(formal) {
            return written;
        }
        // An argumentative-by-name binding registers under both maps, and
        // the arity-checked entry is the one that names the relation.
        if let Some((_, supplied, _, _)) = bindings
            .argumentative_table_refs
            .iter()
            .find(|(param, ..)| param == formal)
        {
            return supplied.clone();
        }
        match bindings.table_params.get(formal) {
            Some(supplied) => SqlIdentifier::new(supplied.clone()),
            // A relation EXPRESSION has no spelling to substitute, and an
            // ordinary alias is not a formal at all: both keep what was
            // written.
            None => written,
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

/// A scalar parameter reference: `A SCALAR PARAMETER IS CODE, NOT DATA`. The
/// term is a bare name and nothing row-dependent; WHICH names qualify is a
/// resolution judgment, not this layer's.
impl<'t> Normalizer<'t> {
    pub(crate) fn scalar_parameter(
        &self,
        node: cst::ScalarParameterReference<'t>,
    ) -> Result<SqlIdentifier> {
        match self.require(node.child(), "a scalar parameter has a spelling")? {
            cst::ScalarParameterReferenceChild::Identifier(name) => Ok(self.identifier(name)),
            cst::ScalarParameterReferenceChild::StroppedForm(name) => {
                Ok(SqlIdentifier::stropped(strop_interior(self.text(name))))
            }
        }
    }

    /// A compile-time integer: a literal, or a definition parameter whose
    /// value is substituted before the ordinary resolved query exists.
    pub(crate) fn compile_time_integer(
        &mut self,
        node: cst::CompileTimeInteger<'t>,
        position: &'static str,
    ) -> Result<i64> {
        match node {
            cst::CompileTimeInteger::Number(number) => {
                let text = self.text(number);
                text.parse::<i64>().map_err(|_| {
                    DelightQLError::parse_error(format!(
                        "{position} takes a whole number; '{text}' is not one"
                    ))
                })
            }
            cst::CompileTimeInteger::ScalarParameterReference(parameter) => {
                self.substituted_integer(self.scalar_parameter(parameter)?, position)
            }
        }
    }

    /// The value a definition parameter was substituted with. Code, not data:
    /// the substitution happens before the ordinary resolved query exists, so
    /// a name with no binding is a refusal here rather than a bind parameter
    /// carried forward.
    fn substituted_integer(&self, name: SqlIdentifier, position: &'static str) -> Result<i64> {
        use crate::pipeline::asts::core::{DomainExpression, LiteralValue};

        // No fabricated stand-in: a bound with nothing to substitute is not a
        // bound of zero. A definition body that cannot be read until its
        // parameters arrive is DEFERRED by the road that owns it, and this
        // refusal is what tells that road so.
        let Some(bindings) = self.features.ho_bindings.as_ref() else {
            return Err(DelightQLError::validation_error_categorized(
                crate::uri_registry::subcat::LIMIT_VALUE,
                format!(
                    "{position} names '{name}', which is an identifier with no active \
                     higher-order scalar binding"
                ),
                "a scalar parameter is code: it is substituted before resolution",
            ));
        };
        match bindings.scalar_params.get(name.as_str()) {
            Some(DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Ground(LiteralValue::Number(
                    number,
                )),
            )) => number.replace('_', "").parse::<i64>().map_err(|_| {
                DelightQLError::validation_error_categorized(
                    crate::uri_registry::subcat::LIMIT_VALUE,
                    format!("{position} takes a whole number; '{name}' is bound to {number}"),
                    "a scalar parameter is code: it is substituted before resolution",
                )
            }),
            Some(_) => Err(DelightQLError::validation_error_categorized(
                crate::uri_registry::subcat::LIMIT_VALUE,
                format!(
                    "{position} takes a whole number; '{name}' is bound to a non-numeric value"
                ),
                "a scalar parameter is code: it is substituted before resolution",
            )),
            None => Err(DelightQLError::validation_error_categorized(
                crate::uri_registry::subcat::LIMIT_VALUE,
                format!(
                    "{position} names '{name}', which is not a scalar parameter of this \
                     higher-order expansion"
                ),
                "a scalar parameter is code: it is substituted before resolution",
            )),
        }
    }
}
