// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// The builder's collection context: HO parameter bindings threaded through
// normalization, and the annotation sidecars a form declares.

use crate::pipeline::asts::core::AuthoredColumn;
use crate::pipeline::asts::core::{NamedReference, Reference};
use std::collections::HashMap;

/// HO parameter bindings threaded through the builder for AST-level substitution.
///
/// When a view body is parsed with HO bindings active, the builder substitutes
/// param names at construction time instead of using text-level regex replacement.
#[derive(Debug, Clone, Default)]
pub struct HoParamBindings {
    /// Glob: param_name → compiler-owned carrier occurrence.
    ///
    /// This binding has no character spelling. The builder places the scope
    /// directly in the unresolved relation and the resolver connects it to
    /// the matching CTE binding.
    pub table_scope_params: HashMap<String, crate::relation::StructuralRelation>,
    /// The one table parameter receiving a piped source, when present.
    /// The formal spelling is diagnostic; the scope is the binding.
    pub pipe_carrier: Option<(String, crate::relation::StructuralRelation)>,
    /// Argumentative: param_name → anonymous table Chain
    pub table_expr_params: HashMap<String, crate::pipeline::asts::unresolved::Chain>,
    /// THE SCALAR FORMALS. A bare name in this set is a PARAMETER of the
    /// definition: the normalizer leaves it standing as a reference (a slot
    /// written with it CONSTRAINS the position rather than binding a fresh
    /// column), and the body's formal frame — the caller-resolved actuals —
    /// answers it at resolution. No caller syntax is substituted.
    pub scalar_formals: std::collections::HashSet<String>,
    /// The scalar formals whose actual is a LITERAL, by value: the one
    /// position that needs a value before resolution — a row bound
    /// (`#< n`) — reads it here, because a literal's encoding is its value.
    pub scalar_literals: HashMap<String, crate::pipeline::asts::core::LiteralValue>,
    /// Pending arity checks for argumentative params that received table references.
    /// (param_name, table_name, expected_column_count, column_names)
    pub argumentative_table_refs: Vec<(String, delightql_types::SqlIdentifier, usize, Vec<String>)>,
    /// Argumentative carrier params: param_name → the declared positional
    /// column names. A glob access of the formal substitutes these as its
    /// caller pattern, so the body sees the supplied table's columns under
    /// the names the DECLARATION gives them — argumentative binding is
    /// positional, and the supplied table's own spellings never reach the
    /// body. A by-name binding needs no map: the body writes the
    /// caller pattern itself, and that pattern IS the binding.
    pub argumentative_patterns: HashMap<String, Vec<String>>,
    /// The relation actuals, each ADMITTED as a closed relation value, to be
    /// bound as carriers before the view body is expanded, with the part of
    /// the call each is bound as. The landing a formal is addressed by is
    /// written into `table_scope_params` by the bind, never ahead of it.
    pub interior_ctes: Vec<(
        String,
        crate::relation::form::HoPart,
        crate::defuse::ClosedRelationActual,
    )>,
}

impl HoParamBindings {
    /// Build a reference to a compiler-owned table carrier without first
    /// converting its identity into a query-local name.
    pub fn table_scope_relation(
        &self,
        formal: &str,
        access: crate::pipeline::asts::unresolved::Access,
        alias: Option<delightql_types::SqlIdentifier>,
        outer: bool,
    ) -> Option<crate::pipeline::asts::unresolved::Chain> {
        self.table_scope_params.get(formal).map(|scope| {
            let access = match (&access, self.argumentative_patterns.get(formal)) {
                (
                    crate::pipeline::asts::unresolved::Access::All
                    | crate::pipeline::asts::unresolved::Access::Unasked,
                    Some(columns),
                ) if !columns.is_empty() => crate::pipeline::asts::unresolved::Access::from_terms(
                    columns
                        .iter()
                        .map(|name| {
                            crate::pipeline::asts::unresolved::DomainExpression::Reference(
                                Reference::Named(NamedReference(AuthoredColumn {
                                    name: name.as_str().into(),
                                    qualifier: None,
                                    namespace_path:
                                        crate::pipeline::asts::unresolved::NamespacePath::empty(),
                                })),
                            )
                        })
                        .collect(),
                ),
                _ => access,
            };
            crate::pipeline::asts::unresolved::Chain::read(
                crate::pipeline::asts::unresolved::Relation::Ground {
                    mention: crate::pipeline::asts::unresolved::GroundMention::Structural {
                        pending: *scope,
                        authored_name: Some(formal.into()),
                        alias,
                    },
                    outer,
                },
                access,
            )
        })
    }
}

/// Context for collecting dangers, options, and DDL blocks
/// during building.
pub struct FeatureCollector {
    dangers: Vec<crate::pipeline::asts::core::DangerSpec>,
    options: Vec<crate::pipeline::asts::core::OptionSpec>,
    ddl_blocks: Vec<crate::pipeline::asts::core::InlineDdlSpec>,
    pub ho_bindings: Option<HoParamBindings>,
    /// An HO definition template is parsed before call-site scalar bindings
    /// exist. Its AST is analysis-only; invocation reparses the source with
    /// real bindings before execution.
    pub(crate) allow_unbound_limit_identifiers: bool,
}

impl std::fmt::Debug for FeatureCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FeatureCollector")
            .field("dangers", &self.dangers)
            .field("options", &self.options)
            .field("ddl_blocks", &self.ddl_blocks)
            .field("ho_bindings", &self.ho_bindings)
            .field(
                "allow_unbound_limit_identifiers",
                &self.allow_unbound_limit_identifiers,
            )
            .finish()
    }
}

impl Default for FeatureCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl FeatureCollector {
    pub fn new() -> Self {
        Self {
            dangers: Vec::new(),
            options: Vec::new(),
            ddl_blocks: Vec::new(),
            ho_bindings: None,
            allow_unbound_limit_identifiers: false,
        }
    }

    /// Create a child collector that inherits ho_bindings but is otherwise fresh.
    pub fn inheriting_ho_bindings(parent: &Self) -> Self {
        let mut fc = Self::new();
        fc.ho_bindings = parent.ho_bindings.clone();
        fc.allow_unbound_limit_identifiers = parent.allow_unbound_limit_identifiers;
        fc
    }

    /// Add a danger spec collected during continuation processing
    pub fn add_danger(&mut self, spec: crate::pipeline::asts::core::DangerSpec) {
        self.dangers.push(spec);
    }

    /// Take collected dangers (leaves the internal vec empty)
    pub fn take_dangers(&mut self) -> Vec<crate::pipeline::asts::core::DangerSpec> {
        std::mem::take(&mut self.dangers)
    }

    /// Add an option spec collected during continuation processing
    pub fn add_option(&mut self, spec: crate::pipeline::asts::core::OptionSpec) {
        self.options.push(spec);
    }

    /// Take collected options (leaves the internal vec empty)
    pub fn take_options(&mut self) -> Vec<crate::pipeline::asts::core::OptionSpec> {
        std::mem::take(&mut self.options)
    }

    /// Add an inline DDL block collected during query parsing
    pub fn add_ddl_block(&mut self, spec: crate::pipeline::asts::core::InlineDdlSpec) {
        self.ddl_blocks.push(spec);
    }

    /// Take collected DDL blocks (leaves the internal vec empty)
    pub fn take_ddl_blocks(&mut self) -> Vec<crate::pipeline::asts::core::InlineDdlSpec> {
        std::mem::take(&mut self.ddl_blocks)
    }
}
