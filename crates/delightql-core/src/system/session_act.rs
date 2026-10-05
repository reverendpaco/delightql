// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The runtime's session acts: a session directive performed on the
//! session's world with the arguments a compiled program supplies.
//!
//! The act is the directive's registered entity: it loads a file, attaches
//! a database, or writes catalog rows, and refuses what the world forbids.
//! What the call means — which directive, its arguments, its receipt — was
//! decided where the program was compiled; the entity's own answer is not
//! read.

use super::DelightQLSystem;
use crate::diagnostic::Internal;
use crate::error::Result;
use crate::pipeline::asts::unresolved::{DomainExpression, FunctionApplication, LiteralValue};

impl DelightQLSystem {
    /// Perform the session act `directive` (its name without `!`) once,
    /// with `arguments` in its declared order, answering the rows the act
    /// reports (none, for an act whose receipt carries no report).
    pub(crate) fn perform_session_act(
        &mut self,
        directive: &str,
        arguments: &[Option<String>],
    ) -> Result<Vec<Vec<Option<String>>>> {
        self.note_effect_executed();
        match directive {
            "retract" => {
                let [Some(entity), Some(namespace), Some(name)] = arguments else {
                    return Err(told(directive));
                };
                let entity: i64 = entity.parse().map_err(|_| told(directive))?;
                self.retract_family(entity, namespace, name)?;
                Ok(Vec::new())
            }
            "consult_tree" => {
                let [Some(path), Some(namespace)] = arguments else {
                    return Err(told(directive));
                };
                crate::bin_cartridge::prelude::consult_tree::consult_tree_act(self, path, namespace)
            }
            "mount_tree" => {
                let [Some(uri), Some(namespace)] = arguments else {
                    return Err(told(directive));
                };
                crate::bin_cartridge::prelude::mount_tree::mount_tree_act(self, uri, namespace)
            }
            "imprint" | "imprint_replace" => {
                let [source, target] = arguments else {
                    return Err(told(directive));
                };
                let verb = format!("{directive}!");
                let results = crate::bin_cartridge::prelude::imprint::imprint_act(
                    self,
                    source.as_deref().unwrap_or_default(),
                    target.as_deref().unwrap_or_default(),
                    directive == "imprint_replace",
                    &verb,
                )?;
                Ok(results
                    .into_iter()
                    .map(|(entity, status, _)| vec![Some(entity), Some(status)])
                    .collect())
            }
            _ => {
                let entity = self.bin_registry().lookup_entity(&format!("{directive}!")).ok_or_else(|| {
                    Internal::invariant(
                        "system::session_act",
                        format!("the session directive '{directive}!' has no registered act"),
                    )
                })?;
                let executable = entity.as_effect_executable().ok_or_else(|| {
                    Internal::invariant(
                        "system::session_act",
                        format!("the session directive '{directive}!' performs no act"),
                    )
                })?;
                let values: Vec<DomainExpression> = arguments
                    .iter()
                    .map(|value| {
                        DomainExpression::Application(FunctionApplication::Ground(match value {
                            Some(text) => LiteralValue::String(text.clone()),
                            None => LiteralValue::Null,
                        }))
                    })
                    .collect();
                executable.execute(&values, None, self)?;
                Ok(Vec::new())
            }
        }
    }
}

/// An act told arguments its compiled statement never writes.
fn told(directive: &str) -> crate::error::DelightQLError {
    Internal::invariant(
        "system::session_act",
        format!("the act '{directive}!' was told arguments of another shape"),
    )
}

/// The namespace `run!("path/to/script.dql")(*)` consults into: the file
/// stem, sanitized to identifier characters. The directive's own syntax
/// names no namespace; the stem leaves the script addressable afterwards,
/// so a consulted script is runnable again through `run_namespace!`.
pub(crate) fn run_namespace_of(path: &str) -> String {
    let stem = std::path::Path::new(path)
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let sanitized: String = stem
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    if sanitized.is_empty() {
        "script".to_string()
    } else {
        sanitized
    }
}

impl DelightQLSystem {
    /// `run!`'s consultation: the file is consulted into its stem's
    /// namespace, or reloaded there when that namespace exists. Answers the
    /// namespace.
    pub(crate) fn consult_for_run(&mut self, path: &str) -> Result<String> {
        let namespace = run_namespace_of(path);
        match crate::host::CompilerHost::namespace_kind(&*self, &namespace)? {
            None => {
                crate::bin_cartridge::prelude::consult::execute_consult(self, path, &namespace, None)?;
            }
            Some(_) => {
                self.reconsult_namespace(&namespace, Some(path))?;
            }
        }
        Ok(namespace)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_run_consults_into_its_file_stem() {
        assert_eq!(super::run_namespace_of("ddl/torture.dql"), "torture");
        assert_eq!(super::run_namespace_of("a/b/my-script.dql"), "my_script");
        assert_eq!(super::run_namespace_of(""), "script");
    }
}
