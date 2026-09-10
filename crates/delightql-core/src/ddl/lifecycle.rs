// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Catalog lifecycle eligibility for the lib→data crossings.
//!
//! `imprint!` is linear: it consumes a live library into its target and
//! archives the source as `{target}::_N_blueprint`, an inert provenance
//! record. Inertness is a catalog fact, never a spelling: the archive ROOT
//! carries `kind = 'blueprint'`, and every descendant keeps its own kind
//! with only its `fq_name` re-rooted, so "is this namespace inert?" is an
//! ancestor-or-self test by exact `::` prefix.
//!
//! This module makes that judgment in one place and publishes it two ways.
//!
//! - As a guard, `refuse_if_blueprint`, for the read-side doors (entity
//!   resolution, function inlining, `enlist!`) where the refusal is the
//!   whole obligation.
//! - As proofs for the materialization side, bound to the catalog they
//!   were judged in. [`Catalog`] is that catalog: the bootstrap connection
//!   locked under a shared borrow of the system, so every lifecycle
//!   mutation (`imprint!`, `consult!`, `unconsult!`, `ground!` — all `&mut`
//!   on the system) is excluded by the borrow checker for as long as any
//!   proof lives. [`LiveNamespace`] witnesses a row of THAT catalog that is
//!   not inert; [`ImprintSource`] witnesses a live library of THAT catalog
//!   eligible to be consumed. Both borrow the catalog, have private fields
//!   and one constructor each, and every read they make goes to the catalog
//!   they hold. The manifest reader (`Manifest::open`) accepts only a
//!   `LiveNamespace` and reads through it; the archival step
//!   (`consume_source_to_blueprint`) accepts only an `ImprintSource` and
//!   writes through it. A proof cannot outlive the lock it was judged
//!   under, cannot survive a consume, and cannot be presented to another
//!   catalog: the judgment is spent, not remembered.

use std::ops::Deref;
use std::sync::MutexGuard;

use rusqlite::{Connection, OptionalExtension};

use crate::diagnostic::{Blueprint, Runtime};
use crate::error::{DelightQLError, Result};

/// The archive root that makes `fq_name` inert, if any.
///
/// `fq_name` is inert iff it equals, or is nested under, some namespace of
/// `kind = 'blueprint'`. Membership is exact string prefix (`==` or
/// `starts_with("{bp}::")`) — the same test that moved the descendants at
/// consumption — never `LIKE`: `_` and `%` are ordinary namespace-name
/// characters, and a LIKE pattern kidnaps prefix siblings (`a_b` vs `acb`).
/// Blueprints are rare, so the scan is a handful of rows.
///
/// The `sys::meta` catalog functor stays VISIBLE because it resolves through
/// `sys::meta`, never through the blueprint path — it does not consult this.
pub(crate) fn blueprint_shadowing(conn: &Connection, fq_name: &str) -> Result<Option<String>> {
    let mut stmt = conn
        .prepare("SELECT fq_name FROM namespace WHERE kind = 'blueprint'")
        .map_err(|e| Runtime::catalog("prepare blueprint inertness scan", e.to_string()))?;
    let blueprints = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| Runtime::catalog("scan blueprint namespaces", e.to_string()))?;
    for bp in blueprints {
        let bp = bp.map_err(|e| Runtime::catalog("read blueprint fq_name", e.to_string()))?;
        if fq_name == bp || fq_name.starts_with(&format!("{}::", bp)) {
            return Ok(Some(bp));
        }
    }
    Ok(None)
}

/// The loud half of [`blueprint_shadowing`]: badged `imprint/blueprint/inert`.
///
/// Read-side doors call this directly; the quiet safety net inside
/// `ConsultRegistry::lookup_entity` uses `blueprint_shadowing` so any other
/// lookup route degrades to a clean not-found rather than executing archived
/// rules. Materialization does not call this: it spends the proofs below.
pub(crate) fn refuse_if_blueprint(conn: &Connection, fq_name: &str) -> Result<()> {
    if let Some(bp) = blueprint_shadowing(conn, fq_name)? {
        // The target the source was consumed into = the blueprint's
        // parent (`{target}::_N_blueprint`); strip the last `::` segment.
        let target = bp.rsplit_once("::").map(|(p, _)| p).unwrap_or(bp.as_str());
        return Err(DelightQLError::from(Blueprint::Inert {
            message: format!(
                "'{}' is an archived blueprint (imprint! consumed it into '{}'); \
                 blueprints are visible but inert — re-consult the source path \
                 for a live copy",
                bp, target
            ),
        }));
    }
    Ok(())
}

/// The catalog a lifecycle judgment is made in: the bootstrap connection,
/// locked, under a shared borrow of the system.
///
/// The one constructor takes the definition-use authority's
/// [`CatalogRead`](crate::defuse::CatalogRead) — a shared borrow of
/// `DelightQLSystem` — and the guard it yields carries that borrow. While a
/// `Catalog` or anything borrowing it exists, no `&mut` operation on the
/// system compiles, so no lifecycle mutation can interleave with a judgment
/// or an act that spends one. The system's own `&mut` roads open one over
/// `&*self` and spend it inside their extent.
///
/// Derefs to the connection for reading and writing; it is never handed out
/// as a bare `&Connection` that could be paired with a proof from elsewhere.
pub(crate) struct Catalog<'s> {
    conn: MutexGuard<'s, Connection>,
}

impl<'s> Catalog<'s> {
    /// Lock the bootstrap store under the read's borrow of the system.
    pub(crate) fn open(read: crate::defuse::CatalogRead<'s>, context: &str) -> Result<Self> {
        Ok(Self {
            conn: read.connection(context)?,
        })
    }

    /// A catalog over a bare store, for the judgment tests only: the
    /// production road is `open`.
    #[cfg(test)]
    pub(crate) fn over(store: &'s std::sync::Mutex<Connection>) -> Self {
        Self {
            conn: store.lock().expect("test store lock"),
        }
    }
}

impl std::fmt::Debug for Catalog<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Catalog")
    }
}

impl Deref for Catalog<'_> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        &self.conn
    }
}

/// A namespace of one [`Catalog`] judged LIVE: the row exists, and neither
/// it nor any ancestor is an imprint archive. Whatever its kind — data,
/// lib, scratch — its definitions and data are the session's to read and
/// bind, in that catalog, for as long as this proof holds it.
///
/// Fields are private; [`LiveNamespace::admit`] is the only constructor.
#[derive(Debug)]
pub(crate) struct LiveNamespace<'c> {
    catalog: &'c Catalog<'c>,
    id: i32,
    fq: String,
    kind: String,
}

impl<'c> LiveNamespace<'c> {
    /// Judge `fq` against `catalog`. `Ok(None)`: no such namespace — the
    /// caller names the absence in its own vocabulary. `Err`: the namespace
    /// is inert (`imprint/blueprint/inert`), or the catalog could not answer.
    ///
    /// A `NULL` kind reads as `unknown`, the catalog's own spelling for a
    /// row whose creation road recorded none.
    pub(crate) fn admit(catalog: &'c Catalog<'c>, fq: &str) -> Result<Option<Self>> {
        let row: Option<(i32, Option<String>)> = catalog
            .query_row(
                "SELECT id, kind FROM namespace WHERE fq_name = ?1",
                [fq],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|e| Runtime::catalog(format!("look up namespace '{}'", fq), e.to_string()))?;
        let Some((id, kind)) = row else {
            return Ok(None);
        };
        refuse_if_blueprint(catalog, fq)?;
        Ok(Some(Self {
            catalog,
            id,
            fq: fq.to_string(),
            kind: kind.unwrap_or_else(|| "unknown".to_string()),
        }))
    }

    /// The catalog this judgment was made in — the only store the proof's
    /// consumers read or write.
    pub(crate) fn catalog(&self) -> &'c Catalog<'c> {
        self.catalog
    }

    pub(crate) fn id(&self) -> i32 {
        self.id
    }

    pub(crate) fn fq(&self) -> &str {
        &self.fq
    }

    pub(crate) fn kind(&self) -> &str {
        &self.kind
    }
}

/// A live library of one [`Catalog`] eligible to be CONSUMED by `imprint!`:
/// a [`LiveNamespace`] whose kind is a library kind (`lib`, or `scratch` —
/// authored in-session, lib-kind by law) and that no grounding currently
/// borrows.
///
/// Kinds are admitted positively. An exclusion list ("not data, not
/// system, not container") is the tempting regression: it silently admits
/// every kind it never named, which is exactly how an archive root
/// (`kind = 'blueprint'`) once materialized again.
///
/// Fields are private; [`ImprintSource::admit`] is the only constructor.
#[derive(Debug)]
pub(crate) struct ImprintSource<'c> {
    live: LiveNamespace<'c>,
}

impl<'c> ImprintSource<'c> {
    /// Judge `fq` as an imprint source of `catalog`. `Ok(None)`: no such
    /// namespace. `Err`: inert, not a library kind, borrowed by a grounding,
    /// or the catalog could not answer. Inertness is judged first, so a data
    /// mount relocated inside an archive answers "inert", not "wrong kind".
    pub(crate) fn admit(catalog: &'c Catalog<'c>, fq: &str) -> Result<Option<Self>> {
        let Some(live) = LiveNamespace::admit(catalog, fq)? else {
            return Ok(None);
        };
        match live.kind() {
            "lib" | "scratch" => {}
            other => {
                return Err(Runtime::catalog(
                    format!(
                        "imprint!() source '{}' is a {} namespace. Source must be a lib namespace.",
                        fq, other
                    ),
                    "Wrong namespace kind",
                ));
            }
        }
        // A borrowed source cannot be consumed: destroying a borrowed
        // resource would dangle the grounding's views. A catalog failure
        // here refuses — it never admits by default.
        let borrowed: bool = catalog
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM grounding WHERE lib_namespace_id = ?1)",
                [live.id()],
                |row| row.get(0),
            )
            .map_err(|e| {
                Runtime::catalog(format!("check groundings of '{}'", fq), e.to_string())
            })?;
        if borrowed {
            return Err(DelightQLError::from(Runtime::General {
                message: format!(
                    "imprint!() cannot consume '{}' — it is borrowed by an active grounding. \
                     Unconsult the grounded namespace first.",
                    fq
                ),
                details: "Source namespace borrowed".to_string(),
            }));
        }
        Ok(Some(Self { live }))
    }

    /// The judged namespace, for opening its manifest.
    pub(crate) fn live(&self) -> &LiveNamespace<'c> {
        &self.live
    }

    /// The catalog this source was judged in — the store the consume
    /// writes.
    pub(crate) fn catalog(&self) -> &'c Catalog<'c> {
        self.live.catalog()
    }

    pub(crate) fn id(&self) -> i32 {
        self.live.id()
    }

    pub(crate) fn fq(&self) -> &str {
        self.live.fq()
    }
}

#[cfg(test)]
mod tests {
    //! The eligibility judgment over a minimal catalog: the archive root,
    //! its descendants of every kind, a prefix sibling that only LOOKS
    //! nested, the library kinds, the non-library kinds, and a borrowed
    //! library. The balls prove the wiring; these pin the judgment.
    //!
    //! The catalog here is a bare store under `Catalog::over`; the
    //! system-borrowing road is `Catalog::open`, and what it excludes is a
    //! borrow-checker fact, not a runtime one: a retained proof or manifest
    //! keeps the system borrowed shared, so `imprint_namespace` (`&mut`)
    //! does not compile beside it.
    use super::*;
    use std::sync::Mutex;

    fn store() -> Mutex<Connection> {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(
            "CREATE TABLE namespace (id INTEGER PRIMARY KEY, name TEXT NOT NULL, pid INTEGER, \
                                     fq_name TEXT, kind TEXT);
             CREATE TABLE grounding (id INTEGER PRIMARY KEY, lib_namespace_id INTEGER);
             INSERT INTO namespace (id, name, pid, fq_name, kind) VALUES
               (1, 'main', NULL, 'main', 'data'),
               (2, 'lib', NULL, 'lib', 'lib'),
               (3, '_0_blueprint', 1, 'main::_0_blueprint', 'blueprint'),
               (4, '_internal', NULL, 'main::_0_blueprint::_internal', 'scratch'),
               (5, 'sub', NULL, 'main::_0_blueprint::sub', 'lib'),
               (6, 'm', NULL, 'main::_0_blueprint::m', 'data'),
               (7, '_0_blueprintx', NULL, 'main::_0_blueprintx', 'lib'),
               (8, 'scr', NULL, 'home::scr', 'scratch'),
               (9, 'g', NULL, 'g', 'grounded'),
               (10, 'nokind', NULL, 'nokind', NULL);",
        )
        .unwrap();
        Mutex::new(c)
    }

    fn inert(result: Result<Option<impl std::fmt::Debug>>, fq: &str) {
        match result {
            Err(e) => {
                let uri = e.error_uri();
                assert!(
                    uri.contains("imprint/blueprint/inert"),
                    "'{fq}': expected the inertness badge, got '{uri}'"
                );
            }
            Ok(v) => panic!("'{fq}' should be inert, got {v:?}"),
        }
    }

    #[test]
    fn live_rows_of_every_kind_are_live() {
        let s = store();
        let c = Catalog::over(&s);
        for fq in ["main", "lib", "home::scr", "g", "nokind"] {
            assert!(
                LiveNamespace::admit(&c, fq).unwrap().is_some(),
                "'{fq}' should be live"
            );
        }
        assert_eq!(
            LiveNamespace::admit(&c, "nokind").unwrap().unwrap().kind(),
            "unknown"
        );
    }

    #[test]
    fn missing_namespace_is_none_not_an_error() {
        let s = store();
        let c = Catalog::over(&s);
        assert!(LiveNamespace::admit(&c, "absent").unwrap().is_none());
        assert!(ImprintSource::admit(&c, "absent").unwrap().is_none());
        // A missing path UNDER an archive is still an absence: nothing to animate.
        assert!(LiveNamespace::admit(&c, "main::_0_blueprint::absent")
            .unwrap()
            .is_none());
    }

    #[test]
    fn archive_root_and_every_descendant_are_inert() {
        let s = store();
        let c = Catalog::over(&s);
        for fq in [
            "main::_0_blueprint",
            "main::_0_blueprint::_internal",
            "main::_0_blueprint::sub",
            "main::_0_blueprint::m",
        ] {
            inert(LiveNamespace::admit(&c, fq), fq);
            inert(ImprintSource::admit(&c, fq), fq);
        }
    }

    #[test]
    fn prefix_sibling_of_an_archive_is_live() {
        // `main::_0_blueprintx` shares a byte prefix with the archive root
        // but is not nested under it: the boundary is `::`, not bytes.
        let s = store();
        let c = Catalog::over(&s);
        assert!(ImprintSource::admit(&c, "main::_0_blueprintx")
            .unwrap()
            .is_some());
    }

    #[test]
    fn library_kinds_are_admitted_positively() {
        let s = store();
        let c = Catalog::over(&s);
        let lib = ImprintSource::admit(&c, "lib").unwrap().unwrap();
        assert_eq!((lib.id(), lib.fq()), (2, "lib"));
        assert!(ImprintSource::admit(&c, "home::scr").unwrap().is_some());
        for fq in ["main", "g", "nokind"] {
            let e = ImprintSource::admit(&c, fq)
                .err()
                .unwrap_or_else(|| panic!("'{fq}' is not a library kind and must be refused"));
            assert!(
                e.to_string().contains("Source must be a lib namespace"),
                "'{fq}': {e}"
            );
        }
    }

    #[test]
    fn borrowed_library_is_not_consumable() {
        let s = store();
        let c = Catalog::over(&s);
        c.execute("INSERT INTO grounding (lib_namespace_id) VALUES (2)", [])
            .unwrap();
        let e = ImprintSource::admit(&c, "lib")
            .err()
            .expect("borrowed must refuse");
        assert!(
            e.to_string().contains("borrowed by an active grounding"),
            "{e}"
        );
        // The borrow is on `lib`, not on the scratch namespace.
        assert!(ImprintSource::admit(&c, "home::scr").unwrap().is_some());
    }

    #[test]
    fn a_missing_grounding_table_refuses_rather_than_admits() {
        let s = store();
        let c = Catalog::over(&s);
        c.execute_batch("DROP TABLE grounding").unwrap();
        assert!(ImprintSource::admit(&c, "lib").is_err());
    }
}
