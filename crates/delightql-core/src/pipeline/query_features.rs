// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// The builder's collection context: HO parameter bindings threaded through
// normalization, and the annotation sidecars a form declares.

/// A reading made with a parameterized use in hand: nothing it reads waits
/// for a later reading.
#[derive(Debug, Clone, Default)]
pub struct HoParamBindings;

/// Context for collecting dangers, options, and DDL blocks
/// during building.
pub struct FeatureCollector {
    dangers: Vec<crate::pipeline::asts::core::DangerSpec>,
    options: Vec<crate::pipeline::asts::core::OptionSpec>,
    /// Blocks that stood before the body of the form being built.
    preamble_blocks: Vec<crate::pipeline::asts::core::InlineDdlSpec>,
    /// Blocks written since the body began.
    ddl_blocks: Vec<crate::pipeline::asts::core::InlineDdlSpec>,
    pub ho_bindings: Option<HoParamBindings>,
}

impl std::fmt::Debug for FeatureCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FeatureCollector")
            .field("dangers", &self.dangers)
            .field("options", &self.options)
            .field("preamble_blocks", &self.preamble_blocks)
            .field("ddl_blocks", &self.ddl_blocks)
            .field("ho_bindings", &self.ho_bindings)
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
            preamble_blocks: Vec::new(),
            ddl_blocks: Vec::new(),
            ho_bindings: None,
        }
    }

    /// Create a child collector that inherits ho_bindings but is otherwise fresh.
    pub fn inheriting_ho_bindings(parent: &Self) -> Self {
        let mut fc = Self::new();
        fc.ho_bindings = parent.ho_bindings.clone();
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

    /// Add an inline DDL block at the position being built.
    pub fn add_ddl_block(&mut self, spec: crate::pipeline::asts::core::InlineDdlSpec) {
        self.ddl_blocks.push(spec);
    }

    /// Add a block from a preamble reached after the body began: an insert
    /// source's own let block, which stands at the chain's leftmost operand
    /// and so before every other part of the statement.
    pub fn add_preamble_block(&mut self, spec: crate::pipeline::asts::core::InlineDdlSpec) {
        self.preamble_blocks.push(spec);
    }

    /// The body begins: every block collected so far stood before it.
    pub fn seal_preamble(&mut self) {
        let written = std::mem::take(&mut self.ddl_blocks);
        self.preamble_blocks.extend(written);
    }

    /// A statement's blocks, by where they stood in it.
    pub fn take_statement_blocks(&mut self) -> crate::pipeline::asts::core::StatementBlocks {
        crate::pipeline::asts::core::StatementBlocks {
            leading: std::mem::take(&mut self.preamble_blocks),
            trailing: std::mem::take(&mut self.ddl_blocks),
        }
    }

    /// Every collected block in authored order, for a form that is not a
    /// program step — a definition, a block body — where a block's place
    /// relative to a body orders nothing.
    pub fn take_ddl_blocks(&mut self) -> Vec<crate::pipeline::asts::core::InlineDdlSpec> {
        let mut blocks = std::mem::take(&mut self.preamble_blocks);
        blocks.append(&mut self.ddl_blocks);
        blocks
    }
}
