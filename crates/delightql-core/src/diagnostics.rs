// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! DelightQL self-diagnostics — see DIAGNOSTICS-DESIGN.md.
//!
//! One provider ships today (`autoloads`), but the finding shape is the
//! design's `DiagnosticFinding` so that `doctor`, `adapters`, `catalog`,
//! etc. slot in later without reshaping the value type. The CLI surfaces
//! this via `dql selftest`.

use crate::system::{DelightQLSystem, LoadPhase, StdlibLoad};

/// Severity of a diagnostic finding. A union, not a bool: "degraded but
/// working" (`Warn`) and "healthy" (`Ok`) must not collapse into one bit —
/// the same information-hole doctrine that replaced the loader's bool with
/// [`StdlibLoad`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Ok,
    Info,
    Warn,
    Error,
}

impl Severity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Severity::Ok => "ok",
            Severity::Info => "info",
            Severity::Warn => "warn",
            Severity::Error => "error",
        }
    }

    /// Glyph for `doctor`-style human output.
    pub fn glyph(&self) -> &'static str {
        match self {
            Severity::Ok => "✓",
            Severity::Info => "i",
            Severity::Warn => "!",
            Severity::Error => "✗",
        }
    }
}

/// One diagnostic finding. `remediation` points at a
/// `delightql-diagnostic://` identifier (a future `dql explain` kind); the
/// `detail` carries the concrete cause meanwhile.
#[derive(Debug, Clone)]
pub struct DiagnosticFinding {
    pub severity: Severity,
    pub provider: String,
    pub summary: String,
    pub detail: Option<String>,
    pub remediation: Option<String>,
}

/// The `autoloads` provider: force-load every embedded stdlib module through
/// the real loader and report each [`StdlibLoad::Failed`] as an `Error`.
/// Stronger than the build-time parse test — it exercises consult too, so a
/// module that parses but fails to register is caught here.
pub fn run_autoloads(system: &DelightQLSystem) -> Vec<DiagnosticFinding> {
    let mut findings = Vec::new();
    for (ns, _src) in crate::stdlib_manifest::STDLIB_MODULES {
        if let StdlibLoad::Failed { phase, error } = system.ensure_stdlib_loaded(ns) {
            let remediation = match phase {
                LoadPhase::Parse => "delightql-diagnostic://autoload/parse_failed",
                LoadPhase::Consult => "delightql-diagnostic://autoload/consult_failed",
            };
            findings.push(DiagnosticFinding {
                severity: Severity::Error,
                provider: "autoloads".to_string(),
                summary: format!("autoload module '{ns}' failed to load"),
                detail: Some(error.to_string()),
                remediation: Some(remediation.to_string()),
            });
        }
        // NotAModule cannot occur (we iterate the manifest); Loaded /
        // AlreadyLoaded are healthy.
    }
    if findings.is_empty() {
        findings.push(DiagnosticFinding {
            severity: Severity::Ok,
            provider: "autoloads".to_string(),
            summary: format!(
                "{} autoload module(s) load cleanly",
                crate::stdlib_manifest::STDLIB_MODULES.len()
            ),
            detail: None,
            remediation: None,
        });
    }
    findings
}

/// The `catalog` provider: every physical system table should have a
/// namespace address (an `activated_entity` row), so it appears in the
/// `sys::` namespace views and not merely via the direct-name schema
/// fallback. Doctrine: everything the compiler/runtime uses is dogfood-
/// exposed — no intentional hidden internals. An un-activated system table
/// is reported as a `Warn` (it is still queryable, just un-namespaced).
pub fn run_catalog(system: &DelightQLSystem) -> Vec<DiagnosticFinding> {
    let conn_arc = system.get_bootstrap_connection();
    let conn = match conn_arc.lock() {
        Ok(c) => c,
        Err(_) => {
            return vec![DiagnosticFinding {
                severity: Severity::Warn,
                provider: "catalog".to_string(),
                summary: "could not inspect catalog (bootstrap connection poisoned)".to_string(),
                detail: None,
                remediation: None,
            }]
        }
    };

    // Physical system tables whose name is not activated into any namespace.
    let orphans: Vec<String> = {
        let mut stmt = match conn.prepare(
            // DISTINCT: a table name with two un-activated entity rows is
            // one orphan, not two (guards the name-dedup false positive
            // SYS-NAMESPACE-TAXONOMY.md flags in the naive check).
            "SELECT DISTINCT e.name FROM entity e
             JOIN entity_type_enum t ON e.type = t.id
             WHERE t.variant = 'DBPermanentTable'
               AND e.name NOT IN (
                   SELECT e2.name FROM activated_entity ae
                   JOIN entity e2 ON e2.id = ae.entity_id
               )
             ORDER BY e.name",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map(|it| it.filter_map(|r| r.ok()).collect())
            .unwrap_or_default();
        rows
    };

    if orphans.is_empty() {
        return vec![DiagnosticFinding {
            severity: Severity::Ok,
            provider: "catalog".to_string(),
            summary: "every system table has a namespace address".to_string(),
            detail: None,
            remediation: None,
        }];
    }

    orphans
        .into_iter()
        .map(|name| DiagnosticFinding {
            severity: Severity::Warn,
            provider: "catalog".to_string(),
            summary: format!("system table '{name}' has no namespace address"),
            detail: Some(format!(
                "'{name}' is queryable by direct name but activated into no sys:: namespace, \
                 so it is invisible to the namespace views. Activate it (import/activation.rs)."
            )),
            remediation: Some("delightql-diagnostic://catalog/orphaned_entity".to_string()),
        })
        .collect()
}

/// Run every diagnostic provider. Today: `autoloads`, `catalog`. Future
/// providers (`adapters`, `identity`, `connectivity`) append here.
pub fn run_selftest(system: &DelightQLSystem) -> Vec<DiagnosticFinding> {
    let mut findings = Vec::new();
    findings.extend(run_autoloads(system));
    findings.extend(run_catalog(system));
    findings
}
