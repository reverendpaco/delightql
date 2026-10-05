// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Target-neutral values exchanged between the compiler and a system host.

use crate::diagnostic::NamespaceName;
use crate::error::{DelightQLError, Result};

/// The user-selected target connection. Host implementations may add other
/// connections, but an unattributed query executes here.
pub(crate) const PRIMARY_CONNECTION_ID: i64 = 2;

/// The runtime database's own connection: the catalog, and the tables a
/// grounded library's companions create.
pub(crate) const BOOTSTRAP_CONNECTION_ID: i64 = 1;

#[derive(Debug, Clone)]
pub(crate) struct PhysicalRead {
    pub(crate) connection_id: i64,
    pub(crate) backend_schema: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct LiminalReceipt {
    pub operation: String,
    pub echoes: Vec<(String, Option<String>)>,
}

#[derive(Debug, Clone)]
pub(crate) enum LiminalRow {
    Directive(LiminalReceipt),
    Define { entity: String },
    Goal { met: bool, goal: String },
}

impl LiminalRow {
    pub fn operation(&self) -> &str {
        match self {
            LiminalRow::Directive(receipt) => &receipt.operation,
            LiminalRow::Define { .. } => "DEFINE",
            LiminalRow::Goal { .. } => "GOAL",
        }
    }

    fn addition_names(&self) -> Vec<&str> {
        match self {
            LiminalRow::Directive(receipt) => receipt
                .echoes
                .iter()
                .map(|(name, _)| name.as_str())
                .collect(),
            LiminalRow::Define { .. } => vec!["entity"],
            LiminalRow::Goal { .. } => vec!["met", "goal"],
        }
    }

    pub fn echoes_json(&self) -> String {
        serde_json::to_string(&self.addition_names()).expect("addition names serialize")
    }

    pub fn receipt_json(&self) -> String {
        let mut object = String::from("{\"success\":1,\"operation\":");
        object.push_str(&serde_json::to_string(self.operation()).expect("operation serializes"));
        let mut member = |name: &str, value: &str| {
            object.push(',');
            object.push_str(&serde_json::to_string(name).expect("addition name serializes"));
            object.push(':');
            object.push_str(value);
        };
        match self {
            LiminalRow::Directive(receipt) => {
                for (name, value) in &receipt.echoes {
                    let value = value
                        .as_ref()
                        .map(|value| serde_json::to_string(value).expect("echo value serializes"))
                        .unwrap_or_else(|| "null".to_string());
                    member(name, &value);
                }
            }
            LiminalRow::Define { entity } => {
                member(
                    "entity",
                    &serde_json::to_string(entity).expect("entity serializes"),
                );
            }
            LiminalRow::Goal { met, goal } => {
                member("met", if *met { "1" } else { "0" });
                member(
                    "goal",
                    &serde_json::to_string(goal).expect("goal serializes"),
                );
            }
        }
        object.push('}');
        object
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ImprintMode {
    Strict,
    Replace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadPhase {
    Parse,
    Consult,
}

#[derive(Debug)]
pub enum StdlibLoad {
    NotAModule,
    AlreadyLoaded,
    Loaded,
    Failed {
        phase: LoadPhase,
        error: DelightQLError,
    },
}

/// The source of the embedded module the manifest lists under a namespace,
/// by the fully qualified name its own row holds.
pub(crate) fn embedded_module(namespace_fq: &str) -> Option<&'static str> {
    crate::stdlib_manifest::STDLIB_MODULES
        .iter()
        .find(|(module, _)| *module == namespace_fq)
        .map(|(_, source)| *source)
}

pub(crate) fn builtin_registry() -> crate::bin_cartridge::registry::BinCartridgeRegistry {
    let mut registry = crate::bin_cartridge::registry::BinCartridgeRegistry::new();
    registry.register_cartridge(crate::bin_cartridge::prelude::create_prelude_cartridge());
    registry.register_cartridge(crate::bin_cartridge::predicates::create_predicates_cartridge());
    registry
}

/// Validate a user-authored namespace without consulting host state.
/// What a producer makes a namespace hold: authored definitions, or a
/// mounted database's tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Producer {
    Library,
    Data,
}

/// THE BACKING CONTRACT's placement law for a producer's target, beside the
/// name guard: `main` is always a leaf, so nothing is created beneath it;
/// authored definitions refuse in `main`; mounted tables refuse in `home`.
pub(crate) fn validate_producer_target(fq: &str, producer: Producer) -> Result<()> {
    let segments: Vec<&str> = fq.split("::").collect();
    let top = segments[0].to_ascii_lowercase();
    let refusal = match (top.as_str(), segments.len(), producer) {
        ("main", 1, Producer::Data) => return Ok(()),
        ("main", 1, Producer::Library) => format!(
            "cannot consult into '{fq}': 'main' is the session's data mount, and authored definitions refuse in it"
        ),
        ("main", _, _) => format!("cannot create namespace '{fq}': 'main' is always a leaf and holds no child namespace"),
        ("home", _, Producer::Data) => format!(
            "cannot mount '{fq}': 'home' holds authored definitions, and a mounted database's tables refuse in it"
        ),
        _ => return Ok(()),
    };
    Err(DelightQLError::from(NamespaceName::Reserved { message: refusal }))
}

pub(crate) fn validate_user_namespace_target(fq: &str) -> Result<()> {
    let segments: Vec<&str> = fq.split("::").collect();
    let top = segments[0];
    let top_lc = top.to_ascii_lowercase();

    for segment in &segments {
        if segment.starts_with('_') {
            return Err(DelightQLError::from(NamespaceName::Reserved {
                message: format!(
                    "cannot create namespace '{}': the segment '{}' begins with '_', \
                     which is reserved for system machinery (e.g. _internal, \
                     _N_blueprint). Choose a name that does not begin with '_'.",
                    fq, segment
                ),
            }));
        }
    }

    if top_lc == "main" || (top_lc == "home" && segments.len() > 1) {
        return Ok(());
    }

    if (top_lc == "sys" || top_lc == "std") && segments.len() > 1 {
        return Err(DelightQLError::from(NamespaceName::SystemSubtree {
            message: format!(
                "cannot create namespace '{}': the '{}::' subtree is reserved for \
                 system machinery. Create your namespace at the top level (or under \
                 home::) instead.",
                fq, top_lc
            ),
        }));
    }

    if matches!(top_lc.as_str(), "sys" | "std" | "home") {
        return Err(DelightQLError::from(NamespaceName::Reserved {
            message: format!(
                "cannot create namespace '{}': '{}' is a reserved system name. \
                 Choose a different top-level name (to author scratch under home, \
                 write home::{}).",
                fq, top_lc, top_lc
            ),
        }));
    }

    if top_lc.starts_with("sys") || top_lc.starts_with("std") {
        return Err(DelightQLError::from(NamespaceName::Reserved {
            message: format!(
                "cannot create namespace '{}': the top-level name '{}' begins with a \
                 reserved system prefix (sys*/std*). Choose a name not beginning with \
                 sys or std (the prefix relaxes under home:: — home::{} is legal).",
                fq, top, top
            ),
        }));
    }

    Ok(())
}
