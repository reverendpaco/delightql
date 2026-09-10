// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// The builder's collection context: HO parameter bindings threaded through
// normalization, and the annotation sidecars a form declares.

use std::collections::HashMap;

/// THE BINDINGS ONE PARAMETERIZED USE SUPPLIES to the body's normalizer:
/// the relation formals, bound by the carrier authority with their
/// receiving interface already applied, and the scalar formals the body's
/// frame answers.
#[derive(Debug, Clone, Default)]
pub struct HoParamBindings {
    /// The relation formals, each bound to the relation it reads AND the
    /// interface it reads it under, as one value the carrier authority
    /// minted. The normalizer reads them; nothing here writes one.
    pub formals: crate::defuse::carriers::RelationFormals,
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
}

impl HoParamBindings {
    /// THE BODY'S READ OF A RELATION FORMAL, under the access the body
    /// wrote. `None` when the name is not a relation formal of this use.
    pub fn formal_read(
        &self,
        formal: &delightql_types::SqlIdentifier,
        access: crate::pipeline::asts::unresolved::Access,
        alias: Option<delightql_types::SqlIdentifier>,
        outer: bool,
    ) -> Option<crate::pipeline::asts::unresolved::Chain> {
        self.formals
            .get(formal)
            .map(|bound| bound.read(access, alias, outer))
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
