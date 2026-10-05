// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! The parser tuple as ONE judged relationship.
//!
//! Three facts are written independently — the generator pin in the
//! Makefile, the runtime and the highlighter in the workspace manifest — and
//! resolved independently by the lockfile. Nothing about their spelling
//! keeps them equal, so the build script judges them before it compiles
//! anything, and the test suite judges the judgment. This file is included
//! by both, so there is one authority and no second copy of the rule.

/// What the workspace says about its parser tuple, as read: the generator
/// version the Makefile pins, and every version the lockfile resolves for the
/// runtime and the highlighter crates (a healthy lockfile resolves each once).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleFacts {
    pub generator_pin: String,
    pub runtimes: Vec<String>,
    pub highlighters: Vec<String>,
}

/// The one version every member of the tuple shares, or why there is none.
///
/// The runtime must be resolved exactly once; the generator pin must equal
/// it (the CLI that writes the parser tables is the crate that reads them);
/// the highlighter, when the lockfile resolves it at all, must equal it too
/// (it reads the same tables through the same runtime types). An absent
/// highlighter is not a split — a checkout that builds no CLI has none.
pub fn judge(facts: &TupleFacts) -> Result<String, String> {
    let runtime = match facts.runtimes.as_slice() {
        [one] => one.clone(),
        [] => return Err("the lockfile resolves no `tree-sitter` runtime".to_string()),
        many => {
            return Err(format!(
                "the lockfile resolves more than one `tree-sitter` runtime: {}",
                many.join(", ")
            ))
        }
    };
    if facts.generator_pin != runtime {
        return Err(format!(
            "the Makefile pins generator CLI {} but the lockfile links runtime {runtime}; \
             the parser tables one writes must be read by the other, so the two move together",
            facts.generator_pin
        ));
    }
    if let Some(other) = facts.highlighters.iter().find(|h| **h != runtime) {
        return Err(format!(
            "the lockfile resolves `tree-sitter-highlight` {other} beside runtime {runtime}; \
             the highlighter reads the runtime's tables and must be the same version"
        ));
    }
    Ok(runtime)
}

/// Every version a lockfile resolves for one package name.
pub fn lockfile_versions(lock: &str, package: &str) -> Vec<String> {
    let mut versions = Vec::new();
    let mut lines = lock.lines();
    while let Some(line) = lines.next() {
        if line.trim() == format!("name = \"{package}\"") {
            if let Some(version) = lines
                .next()
                .and_then(|v| v.trim().strip_prefix("version = \"")?.strip_suffix('"'))
            {
                versions.push(version.to_string());
            }
        }
    }
    versions
}

/// The exact version a workspace manifest pins for one package (`name =
/// "=X"` or `name = { version = "=X", ... }`), for a lockless checkout.
pub fn manifest_exact_pin(manifest: &str, package: &str) -> Option<String> {
    manifest
        .lines()
        .map(str::trim_start)
        .find(|l| l.starts_with(&format!("{package} =")))
        .and_then(|l| {
            let after = l.split_once('=')?.1;
            let quoted = after.split('"').nth(1)?;
            Some(quoted.trim_start_matches('=').to_string())
        })
}

#[cfg(test)]
mod falsifiers {
    use super::*;

    fn agreeing() -> TupleFacts {
        TupleFacts {
            generator_pin: "0.27.0".into(),
            runtimes: vec!["0.27.0".into()],
            highlighters: vec!["0.27.0".into()],
        }
    }

    #[test]
    fn an_agreeing_tuple_names_its_version() {
        assert_eq!(judge(&agreeing()), Ok("0.27.0".to_string()));
    }

    #[test]
    fn a_generator_pin_moved_alone_is_refused() {
        let mut f = agreeing();
        f.generator_pin = "0.28.0".into();
        assert!(judge(&f).unwrap_err().contains("Makefile pins generator CLI 0.28.0"));
    }

    #[test]
    fn a_runtime_moved_alone_is_refused() {
        let mut f = agreeing();
        f.runtimes = vec!["0.28.0".into()];
        assert!(judge(&f).unwrap_err().contains("links runtime 0.28.0"));
    }

    #[test]
    fn a_highlighter_moved_alone_is_refused() {
        let mut f = agreeing();
        f.highlighters = vec!["0.26.13".into()];
        assert!(judge(&f).unwrap_err().contains("tree-sitter-highlight` 0.26.13"));
    }

    #[test]
    fn two_resolved_runtimes_are_refused() {
        let mut f = agreeing();
        f.runtimes = vec!["0.27.0".into(), "0.26.13".into()];
        assert!(judge(&f).unwrap_err().contains("more than one"));
    }

    #[test]
    fn a_checkout_without_the_highlighter_is_whole() {
        let mut f = agreeing();
        f.highlighters.clear();
        assert_eq!(judge(&f), Ok("0.27.0".to_string()));
    }

    #[test]
    fn the_lockfile_and_manifest_readers_read_what_is_written() {
        let lock = "[[package]]\nname = \"tree-sitter\"\nversion = \"0.27.0\"\n\n[[package]]\nname = \"tree-sitter-highlight\"\nversion = \"0.27.0\"\n";
        assert_eq!(lockfile_versions(lock, "tree-sitter"), vec!["0.27.0"]);
        assert_eq!(lockfile_versions(lock, "tree-sitter-highlight"), vec!["0.27.0"]);
        assert!(lockfile_versions(lock, "tree-sitter-language").is_empty());
        let manifest = "tree-sitter = \"=0.27.0\"\ntree-sitter-highlight = { version = \"=0.27.0\", optional = true }\n";
        assert_eq!(manifest_exact_pin(manifest, "tree-sitter").as_deref(), Some("0.27.0"));
        assert_eq!(manifest_exact_pin(manifest, "tree-sitter-highlight").as_deref(), Some("0.27.0"));
    }
}
