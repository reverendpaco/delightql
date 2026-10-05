// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A group delegate `(payload) <~ #(order)` in reduction position
//! (pipe-operators FN.11): every payload value read from the one row of its
//! group the delegate's ordering ranks first, each published under the name
//! it is written with.

use super::Elaborator;
use crate::pipeline::middle::core::decide::grade::CallPosition;
use crate::pipeline::middle::core::node::rel::{referenced_cell, referenced_position};
use crate::pipeline::middle::core::node::{Item, Naming};
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade::DelegateSpec;

impl Elaborator<'_, '_> {
    /// One delegate's payload items, appended to the reduction items `out`;
    /// `keys` are the reduction's grouping keys.
    pub(super) fn delegate(&mut self, spec: &DelegateSpec, keys: &[Item], out: &mut Vec<Item>) -> Result<(), Refusal> {
        let mut payload = Vec::new();
        for item in &spec.payload {
            self.out_items(item, CallPosition::Reduced, &mut payload)?;
        }
        let key_cells: Vec<_> = keys.iter().filter_map(|k| referenced_cell(&self.b, k.expr)).collect();
        let is_key = |item: &Item| referenced_cell(&self.b, item.expr).is_some_and(|cell| key_cells.contains(&cell));
        let key_name = |item: &Item| {
            referenced_position(&self.b, item.expr)
                .and_then(|p| p.answering_name().cloned())
                .map_or_else(|| "the key".to_string(), |n| n.to_string())
        };
        // The grouping position already publishes its key. A selector in the
        // payload covers the rest of the row; a key written again publishes
        // it twice under one name (DUPLICATE AUTHORED NAMES REFUSE); a key
        // written under a new name is a new column.
        let covered: Vec<String> = payload
            .iter()
            .filter(|p| matches!(p.naming, Naming::Glob | Naming::QualifiedGlob) && is_key(p))
            .map(key_name)
            .collect();
        if let Some(repeated) = payload.iter().find(|p| matches!(p.naming, Naming::Reference) && is_key(p)) {
            return Err(refuse::delegate_key_repeated(&key_name(repeated)));
        }
        payload.retain(|p| !(matches!(p.naming, Naming::Glob | Naming::QualifiedGlob) && is_key(p)));
        if payload.is_empty() {
            if let Some(name) = covered.first() {
                return Err(refuse::delegate_key_repeated(name));
            }
        }
        let order = self.order_keys_of(&spec.order)?;
        let key_values: Vec<_> = keys.iter().map(|k| k.expr).collect();
        let values: Vec<_> = payload.iter().map(|p| p.expr).collect();
        let picked = self.b.pick(&key_values, order, &values)?;
        for (item, expr) in payload.into_iter().zip(picked) {
            // A payload column keeps the name its row publishes; a computed
            // payload is named as written.
            let naming = match item.naming {
                Naming::As(name) => Naming::As(name),
                Naming::Reference | Naming::Glob | Naming::QualifiedGlob => {
                    match referenced_position(&self.b, item.expr).and_then(|p| p.answering_name().cloned()) {
                        Some(name) => Naming::As(name),
                        None => Naming::Computed,
                    }
                }
                Naming::Computed | Naming::Abstain => Naming::Computed,
            };
            out.push(Item { expr, naming });
        }
        Ok(())
    }
}
