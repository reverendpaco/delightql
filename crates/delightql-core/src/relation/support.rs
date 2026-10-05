// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use super::PortId;
use crate::pipeline::asts::core::{Phase, TruthExpression};

/// A CORRELATED RESTRICTION: the condition the enclosing join evaluates and
/// the interior occurrences it reads — ONE value.
#[derive(Debug, PartialEq)]
pub struct Correlated<P: Phase> {
    /// The condition the enclosing join evaluates, judged.
    condition: TruthExpression<P>,
    /// The occurrences the condition reads that the relation it stands on
    /// publishes, in the order the condition named them — what the
    /// relation owes.
    inner: Vec<PortId>,
    /// THE RELATION THE RESTRICTION STANDS ON — the one the act recorded
    /// the obligation on. Carried IN the value: every assembly of a step
    /// reads the owner off the payload and refuses a result that is not
    /// it, and a landing reads it too, so whoever holds the value holds
    /// its owner and no road pairs the correlation with another relation.
    standing: crate::relation::SemanticRelation,
}

impl<P: Phase> Clone for Correlated<P> {
    fn clone(&self) -> Self {
        Correlated {
            condition: self.condition.clone(),
            inner: self.inner.clone(),
            standing: self.standing,
        }
    }
}

impl<P: Phase> Correlated<P> {
    /// The condition the enclosing join evaluates, as judged.
    pub(crate) fn condition(&self) -> &TruthExpression<P> {
        &self.condition
    }

    /// Cross a phase boundary, or a same-phase rewrite.
    ///
    /// The evidence the act derived — the occurrences owed, the owner — is
    /// kept only where it still describes the condition: the crossing reads
    /// the occurrences the condition names before and after the rewrite,
    /// through each phase's own reader, and REFUSES a rewrite that changed
    /// them. A walk may re-spell a condition (classify a relation nested in
    /// it, say); a walk that changes which occurrences it reads has changed
    /// what the relation owes, and only the correlation act may derive
    /// that. Whether the destination phase may hold the result at all is
    /// the phases' answer ([`Phase::admit_correlated`]), not the walker's.
    ///
    /// THE REWRITE IS JUDGED AGAIN before the value holds it. The walk is
    /// handed the judged condition and may re-spell any leaf, so what it
    /// answers is a free truth until the destination phase's judgment has
    /// read it against the same interior occurrences; a phase that holds no
    /// correlated restriction refuses there.
    pub(crate) fn crossing<Q: Phase>(
        self,
        condition: impl FnOnce(TruthExpression<P>) -> crate::error::Result<TruthExpression<Q>>,
    ) -> crate::error::Result<Correlated<Q>> {
        let read = P::occurrences_read(&self.condition);
        let condition = condition(self.condition)?;
        let read_after = Q::occurrences_read(&condition);
        if !same_occurrences(&read, &read_after) {
            return Err(crate::diagnostic::Internal::invariant(
                "correlated",
                "a rewrite changed which occurrences a correlated restriction reads: the \
                 support its act derived no longer describes it, and only the act derives \
                 support",
            ));
        }
        Ok(Correlated {
            condition: Q::judge_correlated_condition(condition, &self.inner)?,
            inner: self.inner,
            standing: self.standing,
        })
    }
}

/// The same occurrences, as sets.
fn same_occurrences(before: &[PortId], after: &[PortId]) -> bool {
    before.len() == after.len()
        && before.iter().all(|port| after.contains(port))
        && after.iter().all(|port| before.contains(port))
}

impl<P: Phase> crate::lispy::ToLispy for Correlated<P> {
    fn to_lispy(&self) -> String {
        let inner: Vec<String> = self.inner.iter().map(|port| format!("{port:?}")).collect();
        format!(
            "(correlated {} [{}] {})",
            self.condition.to_lispy(),
            inner.join(" "),
            self.standing.to_lispy()
        )
    }
}

