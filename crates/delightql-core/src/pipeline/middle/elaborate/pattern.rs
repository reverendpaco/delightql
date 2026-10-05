// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A declared pattern (the heading witness of a destructure or a narrowing)
//! as the levels of one expansion: each iteration a level, each binder a
//! bind read by its path from its level's element. The pattern is written,
//! never evaluated: this reads its members and decides nothing about the
//! value it will be applied to, which the expansion's builder judges.

use crate::pipeline::middle::core::heading::Name;
use crate::pipeline::middle::core::node::{Bind, BindAt, BindRole, Level, Reach};
use crate::pipeline::middle::core::refuse::Refusal;
use crate::pipeline::middle::facade::{
    DestructurePattern, IterationPattern, MetadataBinding, Path, PathStep, PatternTarget, RecordPattern,
    RecordPatternMember, TreePattern,
};

/// The levels a pattern makes, each bind under the name it publishes.
pub(super) struct Planned {
    pub(super) levels: Vec<Level>,
}

impl Planned {
    /// How many positions the expansion publishes.
    pub(super) fn width(&self) -> usize {
        self.levels.iter().map(|l| l.binds.len()).sum()
    }

    fn level(&mut self, from: Option<(usize, Option<Path>)>, reach: Reach) -> usize {
        self.levels.push(Level {
            from,
            reach,
            binds: Vec::new(),
        });
        self.levels.len() - 1
    }

    fn bind(&mut self, level: usize, at: BindAt, name: Name) {
        self.levels[level].binds.push(Bind {
            at,
            role: BindRole::Publish(Some(name)),
        });
    }

    fn tree(&mut self, level: usize, prefix: Option<&Path>, tree: &TreePattern) -> Result<(), Refusal> {
        match tree {
            TreePattern::Record(record) => self.record(level, prefix, record),
            TreePattern::Array(array) => {
                for member in array.members.iter() {
                    self.bind(level, BindAt::Path(joined(prefix, &member.path)), Name::new(member.published_name()));
                }
                Ok(())
            }
        }
    }

    fn record(&mut self, level: usize, prefix: Option<&Path>, record: &RecordPattern) -> Result<(), Refusal> {
        for member in record.members.iter() {
            match member {
                RecordPatternMember::Binder(binder) => {
                    let at = joined(prefix, &Path::key(binder.name.to_string()));
                    self.bind(level, BindAt::Path(at), binder.name.clone());
                }
                RecordPatternMember::Keyed { key, binder } => {
                    let at = joined(prefix, &Path::key(key.clone()));
                    self.bind(level, BindAt::Path(at), binder.name.clone());
                }
                RecordPatternMember::Nested { key, target } => {
                    let at = joined(prefix, &Path::key(key.clone()));
                    match &**target {
                        crate::pipeline::middle::facade::NestedPattern::Navigate(tree) => {
                            self.tree(level, Some(&at), tree)?
                        }
                        crate::pipeline::middle::facade::NestedPattern::Iterate(iteration) => {
                            let inner = self.level(Some((level, Some(at))), Reach::Sequence);
                            self.iteration(inner, iteration)?;
                        }
                    }
                }
                RecordPatternMember::Path(binding) => {
                    self.bind(level, BindAt::Path(joined(prefix, &binding.path)), Name::new(binding.published_name()));
                }
                RecordPatternMember::Metadata(binding) => self.metadata(level, prefix.cloned(), binding)?,
                RecordPatternMember::Disregarded => {}
            }
        }
        Ok(())
    }

    fn iteration(&mut self, level: usize, iteration: &IterationPattern) -> Result<(), Refusal> {
        match iteration {
            IterationPattern::Tree(tree) => self.tree(level, None, tree),
            IterationPattern::ScalarArray(binder) => {
                self.bind(level, BindAt::Element, binder.name.clone());
                Ok(())
            }
        }
    }

    /// `key:~> …`: the members of the object here, its keys bound; what
    /// stands under each key read by the target.
    fn metadata(&mut self, level: usize, at: Option<Path>, binding: &MetadataBinding) -> Result<(), Refusal> {
        let keys = self.level(Some((level, at)), Reach::Keys);
        self.bind(keys, BindAt::Key, binding.key.name.clone());
        match &binding.target {
            PatternTarget::Pattern(iteration) => {
                let rows = self.level(Some((keys, None)), Reach::Sequence);
                self.iteration(rows, iteration)
            }
            PatternTarget::Binding(inner) => self.metadata(keys, None, inner),
            PatternTarget::Disregarded => Ok(()),
        }
    }
}

/// A destructure's levels: one row per value for a scalar pattern, one per
/// element for an iterating one.
pub(super) fn destructure(pattern: &DestructurePattern) -> Result<Planned, Refusal> {
    let mut planned = Planned { levels: Vec::new() };
    match pattern {
        DestructurePattern::Scalar(tree) => {
            let root = planned.level(None, Reach::Node);
            planned.tree(root, None, tree)?;
        }
        DestructurePattern::Iterate(iteration) => {
            let root = planned.level(None, Reach::Sequence);
            planned.iteration(root, iteration)?;
        }
    }
    Ok(planned)
}

/// A narrowing destructure's levels: one per element of the sequence.
pub(super) fn narrow(pattern: &RecordPattern) -> Result<Planned, Refusal> {
    let mut planned = Planned { levels: Vec::new() };
    let root = planned.level(None, Reach::Sequence);
    planned.record(root, None, pattern)?;
    Ok(planned)
}

/// A path continued by another.
fn joined(prefix: Option<&Path>, rest: &Path) -> Path {
    match prefix {
        None => rest.clone(),
        Some(prefix) => rest
            .steps()
            .cloned()
            .fold(prefix.clone(), |path, step: PathStep| path.then(step)),
    }
}
