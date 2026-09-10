// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE WALK'S PATH: the context's declared edges as a graph over canonical
//! spellings, and the one simple path between two terms.

use std::collections::HashMap;

use crate::diagnostic::{Constraint, Er, Resolution};
use crate::error::{DelightQLError, Result};

/// Path-finding in the ER graph: enumerate ALL simple paths between the
/// endpoints. Exactly one → that path; zero → no-path error; two or
/// more → the ambiguity error, regardless of relative length — the
/// contract is "if multiple paths exist, the query fails", so a direct
/// edge never silently outranks a longer business path. Enumeration
/// must be exhaustive: a search that stops early (at the shortest, or
/// with a global visited set that suppresses paths sharing an
/// intermediate node) refuses some competitor shapes and silently
/// selects through others, which is worse than either consistent rule.
pub(super) fn bfs_path(
    adjacency: &HashMap<String, Vec<String>>,
    from: &str,
    to: &str,
) -> Result<Vec<String>> {
    if from == to {
        return Err(DelightQLError::from(Er::Endpoint {
            message: "ER-transitive join endpoints must be different tables".to_string(),
        }));
    }

    // ER contexts are hand-authored and small; simple-path enumeration is
    // cheap there. The expansion cap is a refuse-loudly backstop for a
    // pathologically dense context — uniqueness that cannot be verified
    // is reported, never assumed.
    const MAX_EXPANSIONS: usize = 100_000;
    let mut expansions = 0usize;

    let mut found_paths: Vec<Vec<String>> = Vec::new();
    let mut stack: Vec<Vec<String>> = vec![vec![from.to_string()]];

    while let Some(path) = stack.pop() {
        let current = path.last().unwrap();
        if let Some(neighbors) = adjacency.get(current.as_str()) {
            for neighbor in neighbors {
                expansions += 1;
                if expansions > MAX_EXPANSIONS {
                    return Err(DelightQLError::from(Constraint::General {
                        message: format!(
                            "ER-context too dense to verify a unique join path \
                             from '{}' to '{}'; spell the join explicitly with `&`.",
                            from, to,
                        ),
                    }));
                }
                if neighbor == to {
                    let mut p = path.clone();
                    p.push(neighbor.clone());
                    found_paths.push(p);
                } else if !path.contains(neighbor) {
                    let mut p = path.clone();
                    p.push(neighbor.clone());
                    stack.push(p);
                }
            }
        }
    }

    // Deterministic order (shortest first) — the adjacency map is a
    // HashMap, so discovery order is not stable across runs.
    found_paths.sort_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));

    match found_paths.len() {
        0 => Err(DelightQLError::from(Er::EdgeMiss {
            message: format!(
                "No path from '{}' to '{}' in ER-context. \
                 Check that ER-rules connect these tables (directly or transitively).",
                from, to,
            ),
        })),
        1 => Ok(found_paths.into_iter().next().unwrap()),
        _ => {
            let path_strs: Vec<String> = found_paths.iter().map(|p| p.join(" -> ")).collect();
            Err(DelightQLError::from(Resolution::Ambiguous {
                message: format!(
                    "Ambiguous: {} paths from '{}' to '{}':\n  {}",
                    found_paths.len(),
                    from,
                    to,
                    path_strs.join("\n  "),
                ),
            }))
        }
    }
}
