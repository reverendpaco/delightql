// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Paths — the reach INTO a value, and the one form that applies one.
//!
//! A PATH IS SPEC, NOT A VALUE. It never evaluates alone; it travels only
//! into the positions that APPLY it to a source: the JSON accessor
//! (exactly one path) and the pattern side. It is therefore not a domain
//! expression, not a call argument, and not a spread — a consumer that
//! holds one has already been told what it is and never validates it.

use super::super::{Phase, Unresolved};
use super::domain::DomainExpression;
use crate::pipeline::asts::vocabulary::Vec1;
use crate::{lispy::ToLispy, ToLispy};

/// One step of a reach. `.name` and `."quoted key"` are the same step —
/// quoting reaches special characters and says nothing else — while `.0`
/// indexes a container.
#[derive(Debug, Clone, PartialEq, Eq, ToLispy)]
pub enum PathStep {
    /// Object key access: `.field` or `."special-key"`
    #[lispy("path_step:key")]
    Key(String),

    /// Array index access: `.0` (negative indices count from the end)
    #[lispy("path_step:index")]
    Index(i64),
}

impl PathStep {
    /// The step as it was written. A name published from a reach is built
    /// from these, so the spelling of a step has one source.
    pub fn spelling(&self) -> String {
        match self {
            Self::Key(key) => key.clone(),
            Self::Index(index) => index.to_string(),
        }
    }
}

/// A reach, spelled `(. key)+`. Non-empty by construction: a path with no
/// steps reaches nothing, and no surface derives one.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("path")]
pub struct Path(Vec1<PathStep>);

impl Path {
    /// The one door from a possibly-empty step list, and it refuses: a
    /// reach that names nothing is not a shorter path, it is no path.
    pub fn try_from_steps(steps: Vec<PathStep>) -> Option<Self> {
        Vec1::try_from_vec(steps).map(Self)
    }

    pub fn steps(&self) -> impl Iterator<Item = &PathStep> {
        self.0.iter()
    }

    /// The reach in JSON-path suffix spelling: `.key` for a key, `[n]` for
    /// an index. What precedes it — a `$` root, a source column — belongs to
    /// whoever applies the path.
    pub fn suffix(&self) -> String {
        let mut spelling = String::new();
        for step in self.steps() {
            match step {
                PathStep::Key(key) => {
                    spelling.push('.');
                    spelling.push_str(key);
                }
                PathStep::Index(index) => spelling.push_str(&format!("[{index}]")),
            }
        }
        spelling
    }

    /// The same reach as a destructuring MAPPING key: the suffix without a
    /// leading separator, because the mapping's source is named beside it.
    pub fn mapping_key(&self) -> String {
        let suffix = self.suffix();
        suffix.strip_prefix('.').unwrap_or(&suffix).to_string()
    }

    /// The name a reach PUBLISHES when nothing renamed it: `.a.b` → `a_b`.
    pub fn flattened(&self) -> String {
        self.steps()
            .map(PathStep::spelling)
            .collect::<Vec<_>>()
            .join("_")
    }

    /// The last key the reach names, when it ends in one. An
    /// index-terminated reach names no key.
    pub fn last_key(&self) -> Option<&str> {
        match self.steps().last() {
            Some(PathStep::Key(key)) => Some(key.as_str()),
            Some(PathStep::Index(_)) | None => None,
        }
    }
}

/// `x:{.a.b}` — THE one accessor: exactly one path applied to one source.
///
/// The path is a `Path`, so nothing downstream asks whether this node's
/// second child "is a path literal"; the type already answered.
///
/// The source is still the broad value carrier because a definition body
/// may path into a FORMAL (`d:(x) : x:{.name}`), and a formal is spelled as
/// an ordinary reference until the openness landing gives it a typed
/// payload. Narrowing this to a named reference before then would make
/// that body unbuildable.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("json_access")]
pub struct JsonAccess<P: Phase = Unresolved> {
    pub source: Box<DomainExpression<P>>,
    pub path: Path,
}
