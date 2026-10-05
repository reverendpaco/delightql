// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Catalog lifecycle eligibility for the lib→data crossings.
//!
//! `imprint!` is linear: it consumes a live library into its target and
//! archives the source as `{target}::_N_blueprint`, an inert provenance
//! record. Inertness is a catalog fact, never a spelling: the archive ROOT
//! carries `kind = 'blueprint'`, and every descendant keeps its own kind
//! with only its `fq_name` re-rooted, so "is this namespace inert?" is the
//! namespace tree's ancestor-or-self test.
//!
//! This module makes that judgment in one place and publishes it two ways.
//!
//! - As a guard, `refuse_if_blueprint`, for the read-side doors (entity
//!   resolution, function inlining, `enlist!`) where the refusal is the
//!   whole obligation.
//! - As proofs for the reload and materialization side, bound to the
//!   catalog they were judged in. [`Catalog`] is that catalog: the
//!   bootstrap connection locked under a shared borrow of the system, so
//!   every lifecycle mutation (`imprint!`, `consult!`, `unconsult!`,
//!   `ground!` — all `&mut` on the system) is excluded by the borrow
//!   checker for as long as any proof lives. [`LiveNamespace`] witnesses a
//!   row of THAT catalog that is not inert; [`ImprintSource`] witnesses a
//!   live library of THAT catalog eligible to be consumed. Both borrow the
//!   catalog, have private fields and one constructor each, and every read
//!   they make goes to the catalog they hold. The archival step (`consume_source_to_blueprint`) accepts
//!   only an `ImprintSource` and writes through it. The manifest reader
//!   (`ManifestRows::read`) queries the companions through the whole
//!   system, so it holds no lock across its queries: it admits the library
//!   as a `LiveNamespace` before listing them, under the same shared borrow
//!   of the system that excludes every lifecycle mutation while it reads.
//!   A proof cannot outlive the lock it was judged under, cannot survive a
//!   consume, and cannot be presented to another catalog: the judgment is
//!   spent, not remembered.
//!
//! It also owns KIND ADMISSION: which namespace kinds each lifecycle
//! directive acts on. [`admit_kind`] judges the directives that take a
//! namespace by name whatever its liveness (`unmount!`, `unconsult!`,
//! `refresh!`); [`admit_live_kind`] judges the acts that read a namespace's
//! truth into the session (`reconsult!` and both sides of `imprint!`), on a
//! [`LiveNamespace`] whose [`LiveKind`] has no archive value. Both are total
//! matches over a decoded kind: no directive compares kind text, and none
//! has a fall-through.

use std::ops::Deref;
use std::sync::MutexGuard;

use rusqlite::{Connection, OptionalExtension};

use crate::diagnostic::{Blueprint, Runtime};
use crate::error::{DelightQLError, Result};
use crate::namespace::NamespaceKind;

/// The archive root that makes `fq_name` inert, if any.
///
/// `fq_name` is inert iff it is within some namespace of
/// `kind = 'blueprint'`, by [`crate::namespace::is_within`] — the judgment
/// that chose the descendants moved at consumption. Blueprints are rare, so
/// the scan is a handful of rows.
///
/// The `sys::meta` catalog functor stays VISIBLE because it resolves through
/// `sys::meta`, never through the blueprint path — it does not consult this.
pub(crate) fn blueprint_shadowing(conn: &Connection, fq_name: &str) -> Result<Option<String>> {
    let mut stmt = conn
        .prepare("SELECT fq_name FROM namespace WHERE kind = ?1")
        .map_err(|e| Runtime::catalog("prepare blueprint inertness scan", e.to_string()))?;
    let blueprints = stmt
        .query_map([NamespaceKind::Blueprint.spelling()], |r| {
            r.get::<_, String>(0)
        })
        .map_err(|e| Runtime::catalog("scan blueprint namespaces", e.to_string()))?;
    for bp in blueprints {
        let bp = bp.map_err(|e| Runtime::catalog("read blueprint fq_name", e.to_string()))?;
        if crate::namespace::is_within(fq_name, &bp) {
            return Ok(Some(bp));
        }
    }
    Ok(None)
}

/// The loud half of [`blueprint_shadowing`]: badged `imprint/blueprint/inert`.
///
/// Read-side doors call this directly. Materialization does not call this:
/// it spends the proofs below.
pub(crate) fn refuse_if_blueprint(conn: &Connection, fq_name: &str) -> Result<()> {
    match blueprint_shadowing(conn, fq_name)? {
        Some(archive) => Err(inert(&archive)),
        None => Ok(()),
    }
}

/// The inertness refusal, naming the archive that makes a namespace inert.
fn inert(archive: &str) -> DelightQLError {
    // The target the source was consumed into = the archive's parent
    // (`{target}::_N_blueprint`); strip the last `::` segment.
    let target = archive.rsplit_once("::").map(|(p, _)| p).unwrap_or(archive);
    DelightQLError::from(Blueprint::Inert {
        message: format!(
            "'{}' is an archived blueprint (imprint! consumed it into '{}'); \
             blueprints are visible but inert — re-consult the source path \
             for a live copy",
            archive, target
        ),
    })
}

/// The catalog a lifecycle judgment is made in: the bootstrap connection,
/// locked, under a shared borrow of the system.
///
/// The one constructor takes a shared borrow of `DelightQLSystem`, and the
/// guard it yields carries that borrow. While a
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
    pub(crate) fn open(system: &'s crate::system::DelightQLSystem, context: &str) -> Result<Self> {
        Ok(Self {
            conn: system.lock_bootstrap(context)?,
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

/// The kind of a LIVE namespace: every [`NamespaceKind`] but an archive
/// root's. Only [`LiveNamespace::admit`] yields one, after it has refused an
/// archive and everything inside one, so an act judged on a live namespace
/// cannot be asked about an archive at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveKind {
    System,
    Container,
    Data,
    Lib,
    Scratch,
    Grounded,
    Unknown,
}

impl LiveKind {
    /// `None`: `kind` is an archive root's, which is never live.
    fn of(kind: NamespaceKind) -> Option<Self> {
        match kind {
            NamespaceKind::System => Some(LiveKind::System),
            NamespaceKind::Container => Some(LiveKind::Container),
            NamespaceKind::Data => Some(LiveKind::Data),
            NamespaceKind::Lib => Some(LiveKind::Lib),
            NamespaceKind::Scratch => Some(LiveKind::Scratch),
            NamespaceKind::Grounded => Some(LiveKind::Grounded),
            NamespaceKind::Unknown => Some(LiveKind::Unknown),
            NamespaceKind::Blueprint => None,
        }
    }

    /// The catalog kind this live kind is.
    pub(crate) fn kind(self) -> NamespaceKind {
        match self {
            LiveKind::System => NamespaceKind::System,
            LiveKind::Container => NamespaceKind::Container,
            LiveKind::Data => NamespaceKind::Data,
            LiveKind::Lib => NamespaceKind::Lib,
            LiveKind::Scratch => NamespaceKind::Scratch,
            LiveKind::Grounded => NamespaceKind::Grounded,
            LiveKind::Unknown => NamespaceKind::Unknown,
        }
    }
}

impl std::fmt::Display for LiveKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.kind().fmt(f)
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
    kind: LiveKind,
}

impl<'c> LiveNamespace<'c> {
    /// Judge `fq` against `catalog`. `Ok(None)`: no such namespace — the
    /// caller names the absence in its own vocabulary. `Err`: the namespace
    /// is inert (`imprint/blueprint/inert`), its kind does not decode, or the
    /// catalog could not answer.
    ///
    /// An archive root is inert by its own kind; a namespace inside an
    /// archive is inert by the archive that encloses it.
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
        let kind =
            LiveKind::of(NamespaceKind::decode(fq, kind.as_deref())?).ok_or_else(|| inert(fq))?;
        refuse_if_blueprint(catalog, fq)?;
        Ok(Some(Self {
            catalog,
            id,
            fq: fq.to_string(),
            kind,
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

    pub(crate) fn kind(&self) -> LiveKind {
        self.kind
    }
}

/// A live library of one [`Catalog`] eligible to be CONSUMED by `imprint!`:
/// a [`LiveNamespace`] whose kind [`admit_live_kind`] admits as an imprint
/// source (`lib`, or `scratch` — authored in-session, lib-kind by law) and
/// that no grounding currently borrows.
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
        admit_live_kind(LiveVerb::ImprintSource, &live)?;
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

/// A lifecycle directive that takes a namespace by its name whatever the
/// namespace's liveness: the removers, which discard an archive or anything
/// inside one like any other namespace, and `refresh!`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verb {
    Unmount,
    Unconsult,
    Refresh,
}

/// A lifecycle act that reads a namespace's truth into the session: a
/// `reconsult!` reload, or either side of an `imprint!` crossing. It is
/// judged on a [`LiveNamespace`], so an archive and everything inside one
/// have been refused as inert before its kind is consulted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveVerb {
    Reconsult,
    ImprintSource,
    ImprintTarget,
}

/// Whether `verb` acts on the namespace `fq` of `kind`. `Err` is the
/// refusal, naming the directive that does act on that kind where one does.
///
/// Kinds are admitted positively, per verb, with no fall-through: an
/// exclusion list silently admits every kind it never named, and a
/// fall-through that panics poisons the catalog lock it runs under.
pub(crate) fn admit_kind(verb: Verb, fq: &str, kind: NamespaceKind) -> Result<()> {
    use NamespaceKind as K;
    let refuse = |message: String, details: &str| {
        Err(DelightQLError::from(Runtime::General {
            message,
            details: details.to_string(),
        }))
    };
    match verb {
        Verb::Unmount => match kind {
            K::Data => Ok(()),
            K::System
            | K::Container
            | K::Lib
            | K::Scratch
            | K::Grounded
            | K::Blueprint
            | K::Unknown => refuse(
                format!(
                    "Cannot unmount '{fq}' — it is a {kind} namespace. Use unconsult!() for \
                     lib/grounded namespaces."
                ),
                "Wrong namespace kind",
            ),
        },
        Verb::Unconsult => match kind {
            // An imprint archive is an ordinary namespace for removal.
            K::Lib | K::Scratch | K::Grounded | K::Unknown | K::Blueprint => Ok(()),
            K::Data => refuse(
                format!(
                    "Cannot unconsult '{fq}' — it is a data namespace. Use unmount!() instead."
                ),
                "Wrong namespace kind",
            ),
            K::System => refuse(
                format!("Cannot unconsult '{fq}' — system namespaces cannot be removed."),
                "Protected namespace",
            ),
            K::Container => refuse(
                format!(
                    "Cannot unconsult '{fq}' — structural container namespaces cannot be \
                     removed. Unmount or unconsult their child namespaces instead."
                ),
                "Protected namespace",
            ),
        },
        Verb::Refresh => match kind {
            K::Data => Ok(()),
            K::System
            | K::Container
            | K::Lib
            | K::Scratch
            | K::Grounded
            | K::Blueprint
            | K::Unknown => refuse(
                format!(
                    "Cannot refresh '{fq}' — it is a {kind} namespace. refresh!() only works on \
                     data namespaces. Use reconsult!() for lib namespaces."
                ),
                "Wrong namespace kind",
            ),
        },
    }
}

/// Whether `verb` acts on the live namespace `live`, by its kind. `Err` is
/// the refusal, naming the directive that does act on that kind where one
/// does. Total over [`LiveKind`], with no fall-through.
pub(crate) fn admit_live_kind(verb: LiveVerb, live: &LiveNamespace<'_>) -> Result<()> {
    use LiveKind as K;
    let fq = live.fq();
    let kind = live.kind();
    let refuse = |message: String, details: &str| {
        Err(DelightQLError::from(Runtime::General {
            message,
            details: details.to_string(),
        }))
    };
    match verb {
        LiveVerb::Reconsult => match kind {
            K::Lib | K::Scratch | K::Unknown => Ok(()),
            K::Data => refuse(
                format!("Cannot reconsult '{fq}' — it is a data namespace. Use refresh!() instead."),
                "Wrong namespace kind",
            ),
            K::System => refuse(
                format!("Cannot reconsult '{fq}' — system namespaces cannot be modified."),
                "Protected namespace",
            ),
            K::Container => refuse(
                format!(
                    "Cannot reconsult '{fq}' — structural container namespaces have no \
                     authored source. Reconsult their child namespaces instead."
                ),
                "Protected namespace",
            ),
            K::Grounded => refuse(
                format!(
                    "Cannot reconsult '{fq}' — it is a grounded namespace. Reconsult the \
                     source lib namespace instead."
                ),
                "Wrong namespace kind",
            ),
        },
        LiveVerb::ImprintSource => match kind {
            K::Lib | K::Scratch => Ok(()),
            K::System | K::Container | K::Data | K::Grounded | K::Unknown => Err(Runtime::catalog(
                format!("imprint!() source '{fq}' is a {kind} namespace. Source must be a lib namespace."),
                "Wrong namespace kind",
            )),
        },
        LiveVerb::ImprintTarget => match kind {
            K::Data => Ok(()),
            K::System | K::Container | K::Lib | K::Scratch | K::Grounded | K::Unknown => {
                Err(Runtime::catalog(
                    format!(
                        "imprint!() target '{fq}' is a {kind} namespace. Target must be a data \
                         namespace."
                    ),
                    "Wrong namespace kind",
                ))
            }
        },
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
            LiveKind::Unknown
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

    #[test]
    fn a_kind_no_producer_writes_refuses_admission_without_a_panic() {
        let s = store();
        let c = Catalog::over(&s);
        c.execute(
            "INSERT INTO namespace (id, name, fq_name, kind) VALUES (11, 'odd', 'odd', 'archive')",
            [],
        )
        .unwrap();
        let e = LiveNamespace::admit(&c, "odd").unwrap_err();
        assert_eq!(e.error_uri(), "delightql-error://internal/invariant");
    }

    #[test]
    fn every_named_verb_decides_every_kind() {
        use NamespaceKind as K;
        for kind in NamespaceKind::ALL {
            let admitted = |verb| admit_kind(verb, "n", kind).is_ok();
            assert_eq!(
                admitted(Verb::Unmount),
                kind == K::Data,
                "unmount! of {kind}"
            );
            assert_eq!(
                admitted(Verb::Refresh),
                kind == K::Data,
                "refresh! of {kind}"
            );
            assert_eq!(
                admitted(Verb::Unconsult),
                matches!(
                    kind,
                    K::Lib | K::Scratch | K::Grounded | K::Unknown | K::Blueprint
                ),
                "unconsult! of {kind}"
            );
        }
    }

    #[test]
    fn an_archive_is_removed_by_unconsult_and_no_other_named_verb() {
        admit_kind(
            Verb::Unconsult,
            "main::_0_blueprint",
            NamespaceKind::Blueprint,
        )
        .unwrap();
        for verb in [Verb::Unmount, Verb::Refresh] {
            let e = admit_kind(verb, "main::_0_blueprint", NamespaceKind::Blueprint).unwrap_err();
            assert!(
                e.to_string().contains("it is a blueprint namespace"),
                "{verb:?}: {e}"
            );
        }
    }

    #[test]
    fn the_live_verbs_admit_their_kinds_positively() {
        let s = store();
        let c = Catalog::over(&s);
        for (fq, reconsult, source, target) in [
            ("main", false, false, true),
            ("lib", true, true, false),
            ("home::scr", true, true, false),
            ("g", false, false, false),
            ("nokind", true, false, false),
            ("main::_0_blueprintx", true, true, false),
        ] {
            let live = LiveNamespace::admit(&c, fq).unwrap().unwrap();
            for (verb, expected) in [
                (LiveVerb::Reconsult, reconsult),
                (LiveVerb::ImprintSource, source),
                (LiveVerb::ImprintTarget, target),
            ] {
                assert_eq!(
                    admit_live_kind(verb, &live).is_ok(),
                    expected,
                    "{verb:?} of '{fq}'"
                );
            }
        }
    }
}
