// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A pivot `value of key` in reduction position (pipe-operators FN.10): one
//! ordinary reduction item per candidate of the membership witnessing its
//! key, in the candidates' authored order, each named by its candidate and
//! holding the value over the group's rows whose key equals it.

use super::Elaborator;
use crate::pipeline::middle::core::decide::grade::CallPosition;
use crate::pipeline::middle::core::decide::pivot;
use crate::pipeline::middle::core::node::rel::referenced_cell;
use crate::pipeline::middle::core::node::{Consumer, Item, Naming};
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade::{
    CmpOp, DomainExpression, FunctionApplication, PivotSpec, Reference, ValueTemplatePart,
};

impl Elaborator<'_, '_> {
    /// The columns of one pivot, appended to the reduction items `out`;
    /// `keys` are the reduction's grouping keys, `taken` the columns its
    /// earlier pivots publish.
    pub(super) fn pivot(
        &mut self,
        spec: &PivotSpec,
        keys: &[Item],
        taken: &mut Vec<crate::pipeline::middle::core::heading::Name>,
        out: &mut Vec<Item>,
    ) -> Result<(), Refusal> {
        // The key is a column, or a name template over it (FN.10): the
        // template's one interpolation reads the key, its text spells around
        // each candidate.
        let (key_reference, naming) = match &*spec.pivot_key {
            reference @ DomainExpression::Reference(_) => (reference, None),
            DomainExpression::Application(FunctionApplication::Template(template)) => {
                let mut keys = template.parts().filter_map(|part| match part {
                    ValueTemplatePart::Interpolation(value) => Some(&**value),
                    ValueTemplatePart::Text(_) => None,
                });
                let (Some(reference @ DomainExpression::Reference(_)), None) = (keys.next(), keys.next()) else {
                    return Err(pivot::template_key());
                };
                let parts: Vec<Option<String>> = template
                    .parts()
                    .map(|part| match part {
                        ValueTemplatePart::Text(text) => Some(text.clone()),
                        ValueTemplatePart::Interpolation(_) => None,
                    })
                    .collect();
                (reference, Some(parts))
            }
            _ => return Err(pivot::template_key()),
        };
        let key = self.value(key_reference, CallPosition::Value)?;
        let key_name = match key_reference {
            DomainExpression::Reference(Reference::Named(named)) => named.0.name.to_string(),
            _ => "the key".to_string(),
        };
        let stage = *self.stages.last().ok_or_else(|| refuse::elaboration_contract("a pivot outside a stage"))?;
        let run = &self.scopes[stage].run;
        let guards: Vec<_> = run.guards().collect();
        let members = run.bound();
        let values = pivot::witness(&self.b, &guards, key, &self.call_literals)?;
        let names = pivot::columns(&values, taken, &key_name, naming.as_deref())?;
        let mut identifying: Vec<_> = keys.iter().map(|k| k.expr).collect();
        identifying.push(key);
        let single = pivot::single_row(&self.b, &members, &identifying);
        let cells: Vec<_> = keys.iter().filter_map(|k| referenced_cell(&self.b, k.expr)).collect();
        let own = members.iter().map(|(b, _)| *b).collect();
        for (value, name) in values.into_iter().zip(names) {
            let candidate = self.b.constant(value);
            let own_row = self.reads_own_row(&[key, candidate]);
            let column = self.b.cmp(CmpOp::NullSafeEqual, key, candidate, Consumer::Filter, own_row, &self.switches)?;
            let outer = self.column.replace(column);
            let read = self.value(&spec.value_column, CallPosition::Reduction);
            self.column = outer;
            let expr = self.b.pivot_column(column, read?, &cells, &own, single, &self.switches)?;
            out.push(Item {
                expr,
                naming: Naming::As(name),
            });
        }
        Ok(())
    }
}
