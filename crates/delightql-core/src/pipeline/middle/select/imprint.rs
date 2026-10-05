// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE IMPRINTED BODY: while `imprint!` compiles its entities' rules, a rule
//! the manifest lists is another entity, read where it is imprinted. The
//! act compiles the rules at the imprint's derived root; this overlay makes a
//! listed entity read from that world answer with the object it becomes,
//! records which listed entity each rule reads (the creation order), and
//! names the one an entity read before it was declared, so the act compiles
//! that one first.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use super::{Declared, Referent, Referred};
use crate::pipeline::middle::core::heading::Name;
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade;

/// The columns a created object will have: each name (a rule's position may
/// have none) and declared type.
#[derive(Clone, Debug)]
pub(crate) struct Declaration {
    columns: Vec<(Option<String>, Option<String>)>,
}

impl Declaration {
    /// A table declared by its schema: every column named and typed.
    pub(crate) fn of_columns(columns: Vec<(String, Option<String>)>) -> Self {
        Declaration {
            columns: columns.into_iter().map(|(name, ty)| (Some(name), ty)).collect(),
        }
    }

    /// An object made from a compiled rule: the heading it publishes.
    pub(crate) fn of_heading(heading: &[Option<String>]) -> Self {
        Declaration {
            columns: heading.iter().map(|name| (name.clone(), None)).collect(),
        }
    }
}

/// Where the imprint creates, and what its manifest lists.
pub(crate) struct Imprint {
    root: String,
    target: String,
    connection: i64,
    read_schema: Option<String>,
    primary: String,
    /// Each listed entity, and whether it becomes a view.
    listed: Vec<(Name, bool)>,
    progress: RefCell<Progress>,
}

#[derive(Default)]
struct Progress {
    declared: BTreeMap<String, Declaration>,
    compiling: Option<String>,
    unready: Option<String>,
    reads: BTreeMap<String, BTreeSet<String>>,
}

impl Imprint {
    /// `root` is the derived world the rules are compiled in; `target` the
    /// data namespace the objects land in, on `connection`, read through
    /// `read_schema`; `primary` is how the session's primary database is
    /// spelled; `listed` names each entity and whether it becomes a view.
    pub(crate) fn new(
        root: String,
        target: String,
        connection: i64,
        read_schema: Option<String>,
        primary: String,
        listed: impl IntoIterator<Item = (String, bool)>,
    ) -> Self {
        Imprint {
            root,
            target,
            connection,
            read_schema,
            primary,
            listed: listed.into_iter().map(|(name, view)| (Name::new(name), view)).collect(),
            progress: RefCell::new(Progress::default()),
        }
    }

    pub(crate) fn root(&self) -> &str {
        &self.root
    }

    pub(crate) fn declare(&self, entity: &str, declaration: Declaration) {
        self.progress.borrow_mut().declared.insert(entity.to_string(), declaration);
    }

    pub(crate) fn is_declared(&self, entity: &str) -> bool {
        self.progress.borrow().declared.contains_key(entity)
    }

    /// The entity whose rule is compiled next.
    pub(crate) fn begin(&self, entity: &str) {
        let mut progress = self.progress.borrow_mut();
        progress.compiling = Some(entity.to_string());
        progress.unready = None;
    }

    /// The listed entity the last compile read before it was declared.
    pub(crate) fn take_pending(&self) -> Option<String> {
        self.progress.borrow_mut().unready.take()
    }

    /// The listed entities `entity`'s rule reads.
    pub(crate) fn reads_of(&self, entity: &str) -> BTreeSet<String> {
        self.progress.borrow().reads.get(entity).cloned().unwrap_or_default()
    }

    /// Where the SQL of the entity being compiled is stored: a view is kept
    /// by the target's database, which its reads are spelled relative to.
    pub(crate) fn stored_in(&self) -> Option<(Vec<Option<&str>>, &str)> {
        let progress = self.progress.borrow();
        let compiling = progress.compiling.as_deref()?;
        let view = self.listed.iter().any(|(name, view)| *view && *name == compiling);
        view.then(|| (vec![self.read_schema.as_deref()], self.primary.as_str()))
    }

    /// Whether a body standing in `body_namespace` is compiled in this
    /// imprint's world.
    pub(super) fn covers(&self, body_namespace: Option<&str>) -> bool {
        body_namespace.is_some_and(|fq| facade::is_within(fq, &self.root))
    }

    fn listing(&self, name: &Name) -> Option<&Name> {
        let compiling = self.progress.borrow().compiling.clone();
        self.listed
            .iter()
            .map(|(listed, _)| listed)
            .find(|listed| *listed == name && compiling.as_deref().is_none_or(|c| **listed != c))
    }

    /// The judgment of a mention inside this world: a listed entity read
    /// from the root, or named as a free name, is the object it becomes;
    /// a free name the target does not hold is a table missing there.
    pub(super) fn answer(&self, name: &Name, bare: bool, referred: Referred) -> facade::Result<Referred> {
        match referred {
            Referred::Linked(Referent::Family(family)) | Referred::Routed(Referent::Family(family))
                if family.namespace() == self.root && self.listing(family.name()).is_some() =>
            {
                let road = |r| if bare { Referred::Linked(r) } else { Referred::Routed(r) };
                self.object(family.name()).map(road)
            }
            Referred::Grounded(_) | Referred::Unanswered { free: Some(_) } if bare && self.listing(name).is_some() => {
                self.object(name).map(Referred::Linked)
            }
            Referred::Unanswered { free: Some(_) } => Ok(Referred::Unanswered { free: None }),
            other => Ok(other),
        }
    }

    /// The object a listed entity becomes, recorded as read by the entity
    /// being compiled; one not declared yet is named for the act to compile
    /// first.
    fn object(&self, name: &Name) -> facade::Result<Referent> {
        let listed = self.listing(name).cloned().unwrap_or_else(|| name.clone());
        let mut progress = self.progress.borrow_mut();
        if let Some(compiling) = progress.compiling.clone() {
            progress.reads.entry(compiling).or_default().insert(listed.as_str().to_string());
        }
        let Some(declaration) = progress.declared.get(listed.as_str()) else {
            progress.unready = Some(listed.as_str().to_string());
            return Err(unready(listed.as_str()));
        };
        let mut columns = Vec::with_capacity(declaration.columns.len());
        for (position, (column, ty)) in declaration.columns.iter().enumerate() {
            let column = column
                .clone()
                .ok_or_else(|| refuse::imprint_unnamed_read(listed.as_str(), position + 1))?;
            columns.push((column, ty.clone()));
        }
        Ok(Referent::Declared(Declared {
            name: listed,
            namespace: self.target.clone(),
            columns,
            connection: self.connection,
            schema: self.read_schema.clone(),
        }))
    }
}

fn unready(entity: &str) -> Refusal {
    refuse::outside(&format!(
        "a read of the imprinted entity '{entity}' before the object it becomes is declared"
    ))
}
