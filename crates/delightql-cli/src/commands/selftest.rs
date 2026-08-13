// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `dql selftest` — run self-diagnostics against a fresh in-memory system.
//! Machine-facing front door of the diagnostics subsystem (the human
//! `dql doctor` shares the same providers).

use anyhow::Result;
use delightql_core::diagnostics::{DiagnosticFinding, Severity};

pub fn handle_selftest(json: bool, strict: bool) -> Result<()> {
    let findings = run()?;

    if json {
        print_json(&findings);
    } else {
        print_human(&findings);
    }

    // Exit policy: Error always fails; Warn fails
    // only under --strict (brew-doctor advisory posture otherwise).
    let failed = findings
        .iter()
        .any(|f| f.severity == Severity::Error || (strict && f.severity == Severity::Warn));
    if failed {
        std::process::exit(1);
    }
    Ok(())
}

fn run() -> Result<Vec<DiagnosticFinding>> {
    // Fresh CLI handle — selftest inspects dql itself, not a user database.
    let handle = crate::connection::open_handle()?;
    Ok(handle.selftest())
}

fn print_human(findings: &[DiagnosticFinding]) {
    for f in findings {
        println!("[{}] {}: {}", f.severity.glyph(), f.provider, f.summary);
        if let Some(detail) = &f.detail {
            for line in detail.lines() {
                println!("      {line}");
            }
        }
        if let Some(uri) = &f.remediation {
            println!("      fix: {uri}");
        }
    }
    let errors = findings
        .iter()
        .filter(|f| f.severity == Severity::Error)
        .count();
    let warns = findings
        .iter()
        .filter(|f| f.severity == Severity::Warn)
        .count();
    if errors == 0 && warns == 0 {
        println!("\nselftest: all checks passed");
    } else {
        println!("\nselftest: {errors} error(s), {warns} warning(s)");
    }
}

fn print_json(findings: &[DiagnosticFinding]) {
    let arr: Vec<serde_json::Value> = findings
        .iter()
        .map(|f| {
            serde_json::json!({
                "severity": f.severity.as_str(),
                "provider": f.provider,
                "summary": f.summary,
                "detail": f.detail,
                "remediation": f.remediation,
            })
        })
        .collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::Value::Array(arr)).unwrap_or_default()
    );
}
