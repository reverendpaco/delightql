// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Definition instances (W5 #5, #16). Every application is its own
//! instance: its actuals are elaborated once, at the application, and the
//! instance's body is elaborated with each formal bound to them. The one
//! place instance identity decides anything is a recursive definition's
//! self-reference, which is judged by the semantic identity of its actuals
//! (`decide::recursion::reentry`), never by a rendering of rebuilt
//! nodes: nothing is interned, so nothing relates a second elaboration back
//! to a first.

use super::graph::Builder;
use super::ids::{ExprId, InstanceId, RelId};
use super::node::Instance;
use super::refuse::Refusal;

/// What one formal stands for in one instance.
#[derive(Clone, Debug)]
pub(crate) enum Actual {
    /// A scalar formal's value, elaborated in the caller.
    Value(ExprId),
    /// A relation formal's relation, elaborated in the caller: written
    /// among the call's arguments, or `landed` there by a pipe.
    Relation { rel: RelId, landed: bool },
    /// A rule formal's configured value: a definition with its prefix
    /// positions configured and the rest left for the spend.
    Rule(RuleValue),
}

/// A configured rule value (W5 #13, FN.48): the definition it designates
/// and, by position, each configured actual or `None` for a position the
/// spend supplies. A configured value read from the construction rows is a
/// passenger's value, born over `capture`, the construction rows carrying
/// it; a row-free one is its value. A configured relation is a closed
/// relation value.
#[derive(Clone, Debug)]
pub(crate) struct RuleValue {
    pub(crate) definition: String,
    pub(crate) configured: Vec<Option<Actual>>,
    pub(crate) capture: Option<RelId>,
    /// Whether the designator stands inside the body of the definition it
    /// designates, so that a spend returns to that definition while it is
    /// built (recursion-contract-law); a value designated anywhere else is
    /// closed, and each spend is a new instance.
    pub(crate) within_itself: bool,
    /// For a closed value whose definition is nested in definition
    /// instances, the instance formals it captured where it was designated
    /// (an index of the elaborator's closures): an outer formal belongs to
    /// the invocation that closed it, wherever the value is spent.
    pub(crate) closure: Option<usize>,
}

impl Builder {
    /// Record an application's instance once its body is complete. A
    /// written relation actual is a closed relation value
    /// (`decide::capture::relation_actuals`).
    pub(crate) fn push_application(
        &mut self,
        definition: String,
        formals: Vec<Actual>,
        body: RelId,
    ) -> Result<InstanceId, Refusal> {
        super::decide::capture::relation_actuals(self, &formals)?;
        Ok(self.push_instance(Instance::of(definition, formals, body)))
    }
}
