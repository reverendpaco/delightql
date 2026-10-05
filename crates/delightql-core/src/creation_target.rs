// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE CREATION TARGET: the one value naming where a `table!`,
//! `temp_table!` or `temp_view!` object is created.
//!
//! A creation target is judged once, when its effect is planned. The data
//! namespace the designator selected and that namespace's backing come from
//! one catalog row; the connection, the physical placement and, for a
//! session object, the connection-root shadow it is registered under are
//! fields of the same value. The CREATE text, the clash and holder checks,
//! the in-plan note, the receipt, the catalog registration and every later
//! physical read consume the value. None of them asks a connection which
//! namespace it belongs to: several data namespaces share one connection.

use crate::definition_catalog::{BackingFact, DefinitionCatalog, NamespaceKey};
use crate::diagnostic::{EffectDdl, Internal};
use crate::error::{DelightQLError, Result};
use crate::pipeline::compiled_query::{Materialization, Residence};
use crate::pipeline::generator::SqlDialect;
use crate::system_vocabulary::PRIMARY_CONNECTION_ID;

/// The session's default data-write target: an unqualified creation lands
/// here. Without a mount it is backed by the session's in-memory primary.
pub(crate) const DEFAULT_WRITE_TARGET: &str = "main";

/// Where a data namespace's durable objects physically live on its
/// connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DurablePlacement {
    /// A spelled schema: the binding's attach alias or engine schema.
    Schema(String),
    /// The connection's own default schema: the in-memory primary behind an
    /// unmounted `main`, or an engine database opened directly. Whether a
    /// statement can spell it is the engine's question
    /// ([`DurablePlacement::explicit_schema`]).
    EngineDefault,
}

impl DurablePlacement {
    /// The schema a statement spells the durable object with, or `None` when
    /// the engine has no spelling for its default: DuckDB's default schema is
    /// reached unqualified. A spelling is not by itself an exact address:
    /// [`DurablePlacement::address_past_session`] judges that.
    pub(crate) fn explicit_schema(&self, dialect: SqlDialect) -> Option<String> {
        match self {
            DurablePlacement::Schema(schema) => Some(schema.clone()),
            DurablePlacement::EngineDefault => match dialect {
                SqlDialect::SQLite => Some("main".to_string()),
                SqlDialect::PostgreSQL => Some("public".to_string()),
                SqlDialect::DuckDB | SqlDialect::MySQL | SqlDialect::SqlServer => None,
            },
        }
    }

    /// The spelling that reaches the durable object even while the
    /// connection's session pool holds its name, or `None` when the engine
    /// resolves every spelling of this placement into the pool. SQLite's
    /// pool is schema `temp`; PostgreSQL's is `pg_temp`, an alias of the
    /// session's `pg_temp_N`; DuckDB's is schema `main` of catalog `temp`,
    /// which a two-part `main.t` searches before the durable catalog, and
    /// which `temp.t` names outright. An engine with no measured answer has
    /// none.
    pub(crate) fn address_past_session(&self, dialect: SqlDialect) -> Option<String> {
        let schema = self.explicit_schema(dialect)?;
        let reaches_pool = match dialect {
            SqlDialect::SQLite => schema.eq_ignore_ascii_case("temp"),
            SqlDialect::PostgreSQL => schema == "pg_temp" || schema.starts_with("pg_temp_"),
            SqlDialect::DuckDB => {
                schema.eq_ignore_ascii_case("main") || schema.eq_ignore_ascii_case("temp")
            }
            SqlDialect::MySQL | SqlDialect::SqlServer => true,
        };
        (!reaches_pool).then_some(schema)
    }
}

/// The shadow a connection's temp schema is registered under.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ConnectionRoot {
    /// The exact shadow namespace, `sys::shadow::<root>`.
    Rooted(String),
    /// The connection's bindings share no namespace root.
    Unrooted(Vec<String>),
}

/// A data-backed namespace judged as a place objects may be created, from
/// its one backing row. Private fields: only [`DataTarget::read`] builds
/// one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DataTarget {
    namespace_id: i64,
    namespace: String,
    connection_id: i64,
    durable: DurablePlacement,
    /// How reads of the namespace's durable objects are qualified: the
    /// binding's read qualification, which may leave an attach alias
    /// unspelled although writes must spell it.
    read_schema: Option<String>,
    root: ConnectionRoot,
}

impl DataTarget {
    /// THE ONE BACKING JUDGMENT: the namespace `key` names, as a creation
    /// target. Refuses a namespace that is not data-backed rather than
    /// letting creation fall to `main`; `refusing` names the act in the
    /// refusal.
    pub(crate) fn read(
        facts: &dyn DefinitionCatalog,
        key: NamespaceKey<'_>,
        refusing: &str,
    ) -> Result<Self> {
        let Some(fact) = facts.namespace_backing(key)? else {
            let spelled = match key {
                NamespaceKey::Id(id) => format!("namespace #{id}"),
                NamespaceKey::Fq(fq) => format!("'{fq}'"),
            };
            return Err(DelightQLError::from(EffectDdl::TargetNamespace {
                message: format!("{refusing} refuses: {spelled} is not a known namespace"),
            }));
        };
        Self::judge(facts, fact, refusing)
    }

    fn judge(facts: &dyn DefinitionCatalog, fact: BackingFact, refusing: &str) -> Result<Self> {
        if fact.kind != crate::namespace::NamespaceKind::Data {
            return Err(DelightQLError::from(EffectDdl::TargetNamespace {
                message: format!(
                    "{refusing} refuses: '{}' is a {} namespace, not a data-backed one — an \
                     object cannot be created there, and creation never falls back to \
                     '{DEFAULT_WRITE_TARGET}'",
                    fact.fq, fact.kind
                ),
            }));
        }
        let (connection_id, durable, read_schema) = match &fact.mount {
            Some(mount) => {
                if mount.source_uri.starts_with("delightql-bytes://") {
                    return Err(DelightQLError::from(EffectDdl::TargetNamespace {
                        message: format!(
                            "{refusing} refuses: '{}' is backed by an immutable embedded image \
                             ({}) — an object cannot be created there, and creation never \
                             falls back to '{DEFAULT_WRITE_TARGET}'",
                            fact.fq, mount.source_uri
                        ),
                    }));
                }
                let connection_id = mount.connection_id.ok_or_else(|| {
                    corrupt(
                        &fact.fq,
                        "its mount binding's cartridge names no connection",
                    )
                })?;
                let spelled = |value: &Option<String>, what: &str| {
                    value
                        .clone()
                        .ok_or_else(|| corrupt(&fact.fq, &format!("its binding records no {what}")))
                };
                match (mount.qualification.as_str(), mount.class.as_str()) {
                    ("aliased", _) => {
                        let alias = spelled(&mount.attach_alias, "attach alias")?;
                        (
                            connection_id,
                            DurablePlacement::Schema(alias.clone()),
                            Some(alias),
                        )
                    }
                    ("engine_schema", _) => {
                        let schema = spelled(&mount.engine_schema, "engine schema")?;
                        (
                            connection_id,
                            DurablePlacement::Schema(schema.clone()),
                            Some(schema),
                        )
                    }
                    // Reads resolve an unqualified attachment by the engine's
                    // own search; a write cannot, or it lands in the
                    // connection's primary schema.
                    ("unqualified", "attach") => (
                        connection_id,
                        DurablePlacement::Schema(spelled(&mount.attach_alias, "attach alias")?),
                        None,
                    ),
                    // PostgreSQL resolves an unspelled schema through
                    // search_path, so its default is written out.
                    ("unqualified", _) if mount.connection_type == Some(3) => (
                        connection_id,
                        DurablePlacement::Schema("public".to_string()),
                        None,
                    ),
                    ("unqualified", _) => (connection_id, DurablePlacement::EngineDefault, None),
                    (qualification, class) => {
                        return Err(corrupt(
                            &fact.fq,
                            &format!(
                                "its binding is {class}-class with qualification {qualification}"
                            ),
                        ))
                    }
                }
            }
            None if fact.fq == DEFAULT_WRITE_TARGET => {
                (PRIMARY_CONNECTION_ID, DurablePlacement::EngineDefault, None)
            }
            None => {
                return Err(DelightQLError::from(EffectDdl::TargetNamespace {
                    message: format!(
                        "{refusing} refuses: '{}' has no data backing — an object cannot be \
                         created there",
                        fact.fq
                    ),
                }))
            }
        };
        let root = connection_root(facts, connection_id)?;
        Ok(DataTarget {
            namespace_id: fact.namespace_id,
            namespace: fact.fq,
            connection_id,
            durable,
            read_schema,
            root,
        })
    }

    pub(crate) fn connection_id(&self) -> i64 {
        self.connection_id
    }

    pub(crate) fn durable(&self) -> &DurablePlacement {
        &self.durable
    }

    /// The qualification a read of this namespace's durable objects takes
    /// when nothing in the connection's temp schema shares the name.
    pub(crate) fn read_schema(&self) -> Option<&str> {
        self.read_schema.as_deref()
    }

    /// The exact shadow namespace of this namespace's connection, or the
    /// unrelated bindings that leave the connection without a root.
    fn shadow(&self) -> std::result::Result<String, &[String]> {
        match &self.root {
            ConnectionRoot::Rooted(shadow) => Ok(shadow.clone()),
            ConnectionRoot::Unrooted(bindings) => Err(bindings),
        }
    }
}

/// THE CONNECTION-OWNING DATA ROOT, which names a connection's session
/// shadow. Once a session object has registered on the connection, its
/// shadow is the connection's recorded one, whatever mounts have joined or
/// left it since: one physical temp pool keeps one catalog home. Before
/// that, the default write target owns the connection that backs it — its
/// mount's, or the in-memory primary without one — and any other connection
/// is owned by the deepest path every namespace bound to it lies under.
fn connection_root(facts: &dyn DefinitionCatalog, connection_id: i64) -> Result<ConnectionRoot> {
    if let Some(shadow) = facts.recorded_shadow(connection_id)? {
        return Ok(ConnectionRoot::Rooted(shadow));
    }
    let default_backing = match facts.namespace_backing(NamespaceKey::Fq(DEFAULT_WRITE_TARGET))? {
        Some(BackingFact {
            mount: Some(mount), ..
        }) => mount.connection_id,
        Some(BackingFact { mount: None, .. }) | None => Some(PRIMARY_CONNECTION_ID),
    };
    if default_backing == Some(connection_id) {
        return Ok(ConnectionRoot::Rooted(shadow_of(DEFAULT_WRITE_TARGET)));
    }
    let bindings = facts.connection_bindings(connection_id)?;
    let common = crate::namespace::deepest_common_ancestor(bindings.iter().map(String::as_str));
    Ok(match common {
        Some(root) => ConnectionRoot::Rooted(shadow_of(&root)),
        None => ConnectionRoot::Unrooted(bindings),
    })
}

fn shadow_of(root: &str) -> String {
    format!("sys::shadow::{root}")
}

fn corrupt(namespace: &str, what: &str) -> DelightQLError {
    Internal::invariant(
        "creation_target",
        format!("corrupt catalog: data namespace '{namespace}' is bound, but {what}"),
    )
}

/// Where a created object physically lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectPlacement<'t> {
    /// In the target namespace's durable home.
    Durable(&'t DurablePlacement),
    /// In the connection's temp schema, created unqualified.
    TempSchema,
}

/// The target-engine statements that read a created object back: an
/// existence probe, and a column read-back answering each column's name and
/// engine type at the given positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreatedObjectProbe {
    pub(crate) existence: String,
    pub(crate) readback: String,
    pub(crate) name_column: usize,
    pub(crate) type_column: usize,
}

fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// ONE OBJECT A CREATION DIRECTIVE WILL CREATE: the selected data namespace
/// with its backing, the object's name, and what the directive
/// materializes. Private fields; [`CreationTarget::judge`] is the one
/// constructor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreationTarget {
    data: DataTarget,
    name: String,
    materialization: Materialization,
    /// The exact shadow namespace a session object is registered under.
    shadow: Option<String>,
    /// The schema every statement spells the object with — its CREATE and
    /// every read — or `None` when it is written unqualified.
    spelled_schema: Option<String>,
}

impl CreationTarget {
    /// Judge one object on a data target. Refuses a session object on a
    /// connection with no root, and an object the target cannot read back
    /// for registration: creating an object the next statement cannot
    /// resolve is not a successful effect.
    pub(crate) fn judge(
        data: DataTarget,
        name: &str,
        materialization: Materialization,
        operation: &str,
        dialect: SqlDialect,
    ) -> Result<Self> {
        let refusing = format!("{operation}!({}.{name})", data.namespace);
        let shadow = match materialization.residence() {
            Residence::SessionShadow => match data.shadow() {
                Ok(shadow) => Some(shadow),
                Err(bindings) => {
                    return Err(DelightQLError::from(EffectDdl::TargetNamespace {
                        message: format!(
                            "{refusing} refuses: connection {} carries {} — namespaces with no \
                             common root — so a session object there has no connection-root \
                             shadow to be registered under",
                            data.connection_id,
                            bindings.join(", ")
                        ),
                    }))
                }
            },
            Residence::Durable => None,
        };
        // A durable placement the engine would resolve through session
        // state refuses; it is never written unspelled.
        if matches!(
            (dialect, materialization.residence(), &data.durable),
            (
                SqlDialect::PostgreSQL,
                Residence::Durable,
                DurablePlacement::EngineDefault
            )
        ) {
            return Err(DelightQLError::from(EffectDdl::DurableSchemaUnknown {
                message: format!(
                    "{refusing} refuses: the target's schema is unknowable, and a durable \
                     CREATE on postgres must spell its schema — unqualified durable DDL is \
                     search_path-fragile"
                ),
            }));
        }
        let spelled_schema = match materialization.residence() {
            Residence::Durable => data.durable.explicit_schema(dialect),
            Residence::SessionShadow => None,
        };
        let target = CreationTarget {
            data,
            name: name.to_string(),
            materialization,
            shadow,
            spelled_schema,
        };
        if target.probe(dialect).is_none() {
            return Err(DelightQLError::from(
                EffectDdl::CreatedObjectRegistrationUnsupported {
                    message: format!(
                        "{refusing} refuses: this target cannot register created objects; the \
                         object would not resolve by name ({} has no created-object probe \
                         for this placement)",
                        dialect.family_name()
                    ),
                },
            ));
        }
        Ok(target)
    }

    /// THE READ-BACK of the created object, where it was placed: whether it
    /// exists (an exit-skipped creation does not) and its engine columns.
    /// `None` when the engine has no probe for the placement.
    pub(crate) fn probe(&self, dialect: SqlDialect) -> Option<CreatedObjectProbe> {
        let name = self.name.replace('\'', "''");
        let quoted = quote(&self.name);
        match dialect {
            SqlDialect::SQLite => {
                let schema = match self.placement() {
                    ObjectPlacement::TempSchema => "temp".to_string(),
                    ObjectPlacement::Durable(durable) => quote(&durable.explicit_schema(dialect)?),
                };
                let master = match self.placement() {
                    ObjectPlacement::TempSchema => "sqlite_temp_master".to_string(),
                    ObjectPlacement::Durable(_) => format!("{schema}.sqlite_master"),
                };
                Some(CreatedObjectProbe {
                    existence: format!("SELECT type FROM {master} WHERE name = '{name}'"),
                    readback: format!("PRAGMA {schema}.table_info({quoted})"),
                    name_column: 1,
                    type_column: 2,
                })
            }
            SqlDialect::PostgreSQL => {
                let schema = match self.placement() {
                    ObjectPlacement::TempSchema => {
                        "(SELECT nspname FROM pg_namespace WHERE oid = pg_my_temp_schema())"
                            .to_string()
                    }
                    ObjectPlacement::Durable(DurablePlacement::Schema(schema)) => {
                        format!("'{}'", schema.replace('\'', "''"))
                    }
                    ObjectPlacement::Durable(DurablePlacement::EngineDefault) => return None,
                };
                Some(CreatedObjectProbe {
                    existence: format!(
                        "SELECT table_name FROM information_schema.tables \
                         WHERE table_name = '{name}' AND table_schema = {schema}"
                    ),
                    readback: format!(
                        "SELECT c.column_name, c.data_type \
                         FROM information_schema.columns c \
                         WHERE c.table_name = '{name}' AND c.table_schema = {schema} \
                         ORDER BY c.ordinal_position"
                    ),
                    name_column: 0,
                    type_column: 1,
                })
            }
            // DuckDB keeps temp objects in schema `main` of catalog `temp`,
            // so an unqualified name reaches a temp object first. Both
            // probes therefore scope the one catalog and schema the object
            // was placed in — the session's own database for a durable
            // object — and can never answer about two objects.
            SqlDialect::DuckDB => {
                let scope = match self.placement() {
                    ObjectPlacement::TempSchema => "table_catalog = 'temp'".to_string(),
                    ObjectPlacement::Durable(DurablePlacement::Schema(schema)) => format!(
                        "table_catalog = current_database() AND table_schema = '{}'",
                        schema.replace('\'', "''")
                    ),
                    ObjectPlacement::Durable(DurablePlacement::EngineDefault) => {
                        "table_catalog = current_database() AND table_schema = current_schema()"
                            .to_string()
                    }
                };
                Some(CreatedObjectProbe {
                    existence: format!(
                        "SELECT table_name FROM information_schema.tables \
                         WHERE table_name = '{name}' AND {scope}"
                    ),
                    readback: format!(
                        "SELECT column_name, data_type FROM information_schema.columns \
                         WHERE table_name = '{name}' AND {scope} ORDER BY ordinal_position"
                    ),
                    name_column: 0,
                    type_column: 1,
                })
            }
            SqlDialect::MySQL | SqlDialect::SqlServer => None,
        }
    }

    /// A target for crate tests that build plans by hand.
    #[cfg(test)]
    pub(crate) fn for_test(
        namespace: &str,
        namespace_id: i64,
        connection_id: i64,
        durable: DurablePlacement,
        name: &str,
        materialization: Materialization,
    ) -> Self {
        let session = matches!(materialization.residence(), Residence::SessionShadow);
        let spelled_schema = match (&durable, session) {
            (DurablePlacement::Schema(schema), false) => Some(schema.clone()),
            _ => None,
        };
        CreationTarget {
            data: DataTarget {
                namespace_id,
                namespace: namespace.to_string(),
                connection_id,
                durable,
                read_schema: None,
                root: ConnectionRoot::Rooted(shadow_of(DEFAULT_WRITE_TARGET)),
            },
            name: name.to_string(),
            materialization,
            shadow: session.then(|| shadow_of(DEFAULT_WRITE_TARGET)),
            spelled_schema,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn materialization(&self) -> Materialization {
        self.materialization
    }

    /// The selected data namespace: the durable home of a durable object,
    /// the namespace a session object overlays.
    pub(crate) fn namespace(&self) -> &str {
        &self.data.namespace
    }

    pub(crate) fn namespace_id(&self) -> i64 {
        self.data.namespace_id
    }

    pub(crate) fn connection_id(&self) -> i64 {
        self.data.connection_id
    }

    pub(crate) fn placement(&self) -> ObjectPlacement<'_> {
        match self.materialization.residence() {
            Residence::Durable => ObjectPlacement::Durable(&self.data.durable),
            Residence::SessionShadow => ObjectPlacement::TempSchema,
        }
    }

    /// The schema statements spell the object with; `None` is unqualified.
    pub(crate) fn spelled_schema(&self) -> Option<&str> {
        self.spelled_schema.as_deref()
    }

    /// The exact shadow namespace a session object is registered under.
    pub(crate) fn shadow_namespace(&self) -> Option<&str> {
        self.shadow.as_deref()
    }

    /// The namespace the created object's exact catalog path names.
    pub(crate) fn created_namespace(&self) -> &str {
        self.shadow.as_deref().unwrap_or(&self.data.namespace)
    }

    /// The fully qualified selected target, as a receipt reports it.
    pub(crate) fn target_path(&self) -> String {
        format!("{}.{}", self.data.namespace, self.name)
    }

    /// The exact catalog path of the object created, as a receipt reports
    /// it.
    pub(crate) fn created_path(&self) -> String {
        format!("{}.{}", self.created_namespace(), self.name)
    }

}

#[cfg(test)]
mod tests {
    use super::{CreationTarget, DurablePlacement};
    use crate::pipeline::asts::effects::DirectiveKind;
    use crate::pipeline::compiled_query::Materialization;
    use crate::pipeline::generator::SqlDialect;

    fn target(durable: DurablePlacement, name: &str, directive: DirectiveKind) -> CreationTarget {
        CreationTarget::for_test(
            "data::n",
            40,
            2,
            durable,
            name,
            Materialization::of_directive(directive).expect("a materializing directive"),
        )
    }

    /// The probe reads the object where it was placed — never both places
    /// with a preference between them.
    #[test]
    fn the_probe_reads_the_placement_the_target_names() {
        let durable = target(
            DurablePlacement::Schema("_imported_7".to_string()),
            "staged",
            DirectiveKind::Table,
        )
        .probe(SqlDialect::SQLite)
        .expect("sqlite durable probe");
        assert_eq!(
            durable.existence,
            "SELECT type FROM \"_imported_7\".sqlite_master WHERE name = 'staged'"
        );
        assert_eq!(
            durable.readback,
            "PRAGMA \"_imported_7\".table_info(\"staged\")"
        );
        assert_eq!((durable.name_column, durable.type_column), (1, 2));

        let temp = target(
            DurablePlacement::EngineDefault,
            "staged",
            DirectiveKind::TempTable,
        )
        .probe(SqlDialect::SQLite)
        .expect("sqlite temp probe");
        assert_eq!(
            temp.existence,
            "SELECT type FROM sqlite_temp_master WHERE name = 'staged'"
        );
        assert_eq!(temp.readback, "PRAGMA temp.table_info(\"staged\")");

        let primary = target(
            DurablePlacement::EngineDefault,
            "staged",
            DirectiveKind::Table,
        )
        .probe(SqlDialect::SQLite)
        .expect("sqlite in-memory primary probe");
        assert_eq!(primary.readback, "PRAGMA \"main\".table_info(\"staged\")");
    }

    #[test]
    fn the_postgres_probe_scopes_one_schema() {
        let durable = target(
            DurablePlacement::Schema("public".to_string()),
            "dur",
            DirectiveKind::Table,
        )
        .probe(SqlDialect::PostgreSQL)
        .expect("postgres durable probe");
        assert!(
            durable.existence.contains("table_schema = 'public'"),
            "{}",
            durable.existence
        );
        assert!(
            !durable.existence.contains("pg_my_temp_schema"),
            "{}",
            durable.existence
        );
        assert!(durable.readback.contains("ORDER BY c.ordinal_position"));
        assert_eq!((durable.name_column, durable.type_column), (0, 1));
        let temp = target(
            DurablePlacement::Schema("public".to_string()),
            "dur",
            DirectiveKind::TempView,
        )
        .probe(SqlDialect::PostgreSQL)
        .expect("postgres temp probe");
        assert!(
            temp.readback.contains("pg_my_temp_schema()"),
            "{}",
            temp.readback
        );
        assert!(!temp.readback.contains("'public'"), "{}", temp.readback);
        assert!(
            target(DurablePlacement::EngineDefault, "dur", DirectiveKind::Table)
                .probe(SqlDialect::PostgreSQL)
                .is_none(),
            "an unspelled durable schema on postgres has no probe"
        );
        let evil = target(
            DurablePlacement::Schema("public".to_string()),
            "a'b",
            DirectiveKind::Table,
        )
        .probe(SqlDialect::PostgreSQL)
        .expect("postgres probe with a quote");
        assert!(evil.existence.contains("'a''b'"), "{}", evil.existence);
    }

    #[test]
    fn engines_without_a_probe_cannot_register() {
        for dialect in [SqlDialect::MySQL, SqlDialect::SqlServer] {
            assert!(
                target(DurablePlacement::EngineDefault, "t", DirectiveKind::Table)
                    .probe(dialect)
                    .is_none()
            );
        }
    }

    /// DuckDB's temp objects stand first for an unqualified name, so both
    /// probes scope the catalog and schema the object was placed in: a
    /// durable read-back can never describe a same-named temp object.
    #[test]
    fn duckdb_probes_scope_the_placed_catalog() {
        let temp = target(
            DurablePlacement::EngineDefault,
            "t",
            DirectiveKind::TempTable,
        )
        .probe(SqlDialect::DuckDB)
        .expect("duckdb temp probe");
        for sql in [&temp.existence, &temp.readback] {
            assert!(sql.contains("table_catalog = 'temp'"), "{sql}");
        }
        let durable = target(DurablePlacement::EngineDefault, "t", DirectiveKind::Table)
            .probe(SqlDialect::DuckDB)
            .expect("duckdb durable probe");
        for sql in [&durable.existence, &durable.readback] {
            assert!(
                sql.contains("table_catalog = current_database()")
                    && sql.contains("table_schema = current_schema()"),
                "{sql}"
            );
            assert!(!sql.contains("PRAGMA"), "{sql}");
        }
        assert!(durable.readback.contains("information_schema.columns"));
        assert_eq!((durable.name_column, durable.type_column), (0, 1));
        let spelled = target(
            DurablePlacement::Schema("s".to_string()),
            "t",
            DirectiveKind::Table,
        )
        .probe(SqlDialect::DuckDB)
        .expect("duckdb spelled-schema probe");
        assert!(
            spelled.readback.contains("table_schema = 's'"),
            "{}",
            spelled.readback
        );
    }

    #[test]
    fn receipts_name_the_target_and_the_created_object() {
        let durable = target(
            DurablePlacement::Schema("_imported_7".to_string()),
            "t",
            DirectiveKind::Table,
        );
        assert_eq!(durable.target_path(), "data::n.t");
        assert_eq!(durable.created_path(), "data::n.t");
        let temp = target(
            DurablePlacement::EngineDefault,
            "t",
            DirectiveKind::TempTable,
        );
        assert_eq!(temp.target_path(), "data::n.t");
        assert_eq!(temp.created_path(), "sys::shadow::main.t");
    }

    fn roots(bindings: &[&str]) -> Option<String> {
        crate::namespace::deepest_common_ancestor(bindings.iter().copied())
    }

    #[test]
    fn a_connection_root_is_the_deepest_path_every_binding_shares() {
        assert_eq!(roots(&["pg"]).as_deref(), Some("pg"));
        assert_eq!(
            roots(&["warehouse::public", "warehouse::sales"]).as_deref(),
            Some("warehouse")
        );
        assert_eq!(roots(&["a", "b"]), None);
        assert_eq!(roots(&[]), None);
    }

}
