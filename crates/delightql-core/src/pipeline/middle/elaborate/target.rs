// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Relations the target's engine serves and the catalog does not record:
//! an engine reference `main/x` and a target table function `f(…)(…)`. Each
//! heading is the engine's own, answered by its introspection where the
//! statement is compiled; DQL never catalogs the name.

use super::instances::call_term;
use super::{Elaborator, Term};
use crate::pipeline::middle::core::decide::grade::CallPosition;
use crate::pipeline::middle::core::heading::Name;
use crate::pipeline::middle::core::ids::{ExprId, RelId};
use crate::pipeline::middle::core::node::run::MergeRequest;
use crate::pipeline::middle::core::node::{CatalogColumn, Physical, Route};
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade::{
    Access, CallArguments, EngineRelation, FunctorCall, HoArgument, QualifiedName, Qualifier, ScalarArgument, Slot,
};

/// Which target callable a table-function call names.
pub(super) enum TargetCallee {
    /// An unqualified name no DQL entity answers, applied to values alone.
    /// The refusal is that selection's, raised when the statement or the
    /// catalog claims the name after all.
    Open(Refusal),
    /// `sys::target.f`: the target provider's, past any DQL identity of the
    /// name.
    Provider,
    /// `main/f`: the engine's own, its heading read in the catalog of the
    /// database backing the route.
    Engine(Qualifier),
}

impl Elaborator<'_, '_> {
    /// THE ENGINE'S CATALOG IS THE ENGINE'S: `main/x` reads `x` in the engine's
    /// own catalog of the database backing `main`, and `s/x` reads `x` in the
    /// target engine's schema `s`, whatever DQL's tiers hold.
    pub(super) fn engine_read(&mut self, identifier: &QualifiedName, access: Option<&Access>) -> Result<RelId, Refusal> {
        let (schema, connection, backing) = self.engine_backing(&identifier.namespace_path.qualifier())?;
        let name = &identifier.name;
        let Some(engine) = self.input.engine_relation(connection, Some(&backing), name.as_str())? else {
            return Err(refuse::no_engine_heading(&format!("{schema}/{name}")));
        };
        let (columns, physical) = self.engine_columns(engine);
        let access = self.access_spec(access, columns.len())?;
        self.b.catalog_read(name.clone(), schema, None, columns, physical, false, access, &self.switches)
    }

    /// Where an engine route `s/…` reaches: the route as written, the
    /// connection backing `main`, and the engine schema. `main` is the
    /// schema of the database backing DQL's `main`; any other identifier
    /// before the slash is a schema of that target engine, never a DQL
    /// namespace (top-grammar FN.1).
    fn engine_backing(&self, route: &Qualifier) -> Result<(String, i64, String), Refusal> {
        let schema = self.input.qualifier_spelled(route);
        // Where `main` lives is the catalog's binding, read as data; a
        // binding read unqualified is spelled past the session pool, as a
        // durable read past a session object is.
        let (connection, bound) = self.input.namespace_schema("main")?;
        if schema != "main" {
            return Ok((schema.clone(), connection, schema));
        }
        let backing = match bound {
            Some(spelled) => spelled,
            None => self.input.past_session_schema(&schema, connection)?.ok_or_else(|| {
                refuse::outside("an engine reference on a target with no schema spelling past its session pool")
            })?,
        };
        Ok((schema, connection, backing))
    }

    /// An engine relation's columns as the core reads them, and where its
    /// rows are read. Its storage is the engine's, so no column is known to
    /// hold its declared type, and none is written.
    fn engine_columns(&self, engine: EngineRelation) -> (Vec<CatalogColumn>, Physical) {
        let columns = engine
            .columns
            .into_iter()
            .map(|(name, declared)| CatalogColumn {
                class: self.input.declared_class(declared.as_deref()),
                name,
                declared,
                computed: None,
                stored: false,
            })
            .collect();
        let physical = Physical {
            connection: engine.connection,
            schema: engine.schema,
        };
        (columns, physical)
    }

    /// A target table function applied to its arguments, as `callee` names
    /// it. Its heading is the function's own, and its access is the one
    /// access law's: one slot per column the function delivers, by position.
    pub(super) fn function_term(
        &mut self,
        call: &FunctorCall,
        access: Option<&Access>,
        merge: MergeRequest,
        alias: Option<Name>,
        callee: TargetCallee,
    ) -> Result<Term, Refusal> {
        let name = self.input.callee_name(&call.callee).0;
        let primary = crate::pipeline::middle::facade::PRIMARY_CONNECTION;
        let (connection, schema) = match callee {
            // A name the statement's blocks or formals claim, or the catalog
            // answers, is no target function: the selection's refusal stands.
            TargetCallee::Open(unknown) => {
                if self.claimed_kind(&name).is_some()
                    || self.formal_relation(&name).is_some()
                    || self.refer(&name, None)?.is_some()
                {
                    return Err(unknown);
                }
                (primary, None)
            }
            // The provider is not a schema: its call is spelled bare.
            TargetCallee::Provider => (primary, None),
            TargetCallee::Engine(route) => {
                let (_, connection, backing) = self.engine_backing(&route)?;
                (connection, Some(backing))
            }
        };
        let engine = match self.input.engine_relation(connection, schema.as_deref(), name.as_str())? {
            Some(engine) => engine,
            // A relation with no catalogued heading still follows default
            // transpilation, and a target disagreement is the target's own
            // runtime error (caveat emptor). A complete listed pattern of
            // distinct names declares the heading the call publishes; a read
            // with no such declaration carries an opaque heading, which this
            // middle does not represent.
            None => {
                let declared: Option<Vec<Name>> = match access {
                    Some(Access::Slots(slots)) => slots
                        .iter()
                        .map(|slot| match slot {
                            Slot::Bind(binder) => Some(binder.name.clone()),
                            Slot::Anon | Slot::Reuse(_) | Slot::Constraint(_) => None,
                        })
                        .collect(),
                    _ => None,
                };
                match declared {
                    Some(names) if names.iter().enumerate().all(|(i, n)| !names[..i].contains(n)) => EngineRelation {
                        connection,
                        schema,
                        columns: names.into_iter().map(|n| (n, None)).collect(),
                    },
                    _ => {
                        return Err(refuse::outside(
                            "a table function the target serves no heading of, read without a pattern naming each of \
                             its columns once (an opaque heading)",
                        ))
                    }
                }
            }
        };
        if !matches!(merge, MergeRequest::None) {
            return Err(refuse::outside("a table function merged by name with the run"));
        }
        let (columns, physical) = self.engine_columns(engine);
        let spec = self.access_spec(access, columns.len())?;
        let args = self.function_arguments(&call.arguments)?;
        let rel = self.b.function_read(name.clone(), args, columns, physical, spec, &self.switches)?;
        Ok(call_term(rel, call, access, alias, name, Route::Plain, MergeRequest::None))
    }

    /// A table function's arguments: values, each where the call stands.
    fn function_arguments(&mut self, arguments: &CallArguments) -> Result<Vec<ExprId>, Refusal> {
        let mut out = Vec::new();
        match arguments {
            CallArguments::None => {}
            CallArguments::Scalar(args) => {
                for arg in args {
                    match arg {
                        ScalarArgument::Value(value) if !value.distinct => {
                            out.push(self.value(&value.value, CallPosition::Value)?)
                        }
                        _ => return Err(refuse::outside("a table function argument that is not a value")),
                    }
                }
            }
            CallArguments::HigherOrder(part) => {
                for member in part.members() {
                    match member {
                        HoArgument::Value(value) if !value.distinct => {
                            out.push(self.value(&value.value, CallPosition::Value)?)
                        }
                        _ => return Err(refuse::outside("a table function argument that is not a value")),
                    }
                }
            }
        }
        Ok(out)
    }
}
