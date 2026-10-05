// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Target-neutral facts supplied by a definition catalog.
//!
//! The compiler owns every reach and selection judgment. A host may provide
//! these closed storage facts, or refuse the catalog capability; it never
//! receives a semantic question to answer and never exposes its concrete
//! system through this interface.

use crate::error::Result;
use crate::namespace::NamespaceKind;

#[derive(Debug)]
pub(crate) struct NamespaceFact {
    pub(crate) id: i64,
    pub(crate) fq: Option<String>,
    pub(crate) kind: NamespaceKind,
    pub(crate) default_data_ns: Option<String>,
}

#[derive(Debug)]
pub(crate) struct CandidateFact {
    pub(crate) entity_id: i64,
    pub(crate) cartridge_id: i64,
    pub(crate) namespace_id: i64,
    pub(crate) name: String,
    pub(crate) stropped: bool,
    pub(crate) kind: crate::enums::EntityType,
    pub(crate) definition: Option<String>,
    pub(crate) namespace: String,
    pub(crate) source_uri: Option<String>,
}

/// An edge entity activated in a namespace, with the selection keys its
/// first clause stored: the pair's canonical spellings and its context.
#[derive(Debug)]
pub(crate) struct EdgeDeclarationFact {
    pub(crate) candidate: CandidateFact,
    pub(crate) left: String,
    pub(crate) right: String,
    pub(crate) context: String,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ImportOwner {
    Namespace(i64),
    Cartridge(i64),
}

/// Which namespace row a backing fact is read for.
#[derive(Debug, Clone, Copy)]
pub(crate) enum NamespaceKey<'a> {
    Id(i64),
    Fq(&'a str),
}

/// ONE namespace row with the mount binding that backs it, read together:
/// the row and its binding are never looked up separately and paired.
#[derive(Debug, Clone)]
pub(crate) struct BackingFact {
    pub(crate) namespace_id: i64,
    pub(crate) fq: String,
    pub(crate) kind: NamespaceKind,
    pub(crate) mount: Option<MountFact>,
}

/// A namespace's mount binding and the connection its cartridge runs on.
#[derive(Debug, Clone)]
pub(crate) struct MountFact {
    pub(crate) class: String,
    pub(crate) qualification: String,
    pub(crate) attach_alias: Option<String>,
    pub(crate) engine_schema: Option<String>,
    pub(crate) source_uri: String,
    pub(crate) connection_id: Option<i64>,
    pub(crate) connection_type: Option<i64>,
}

/// A session object holding a physical temp name on one connection.
#[derive(Debug, Clone)]
pub(crate) struct SessionHolderFact {
    pub(crate) kind: i32,
    /// The recorded durable owner; `None` when that namespace is gone.
    pub(crate) owner: Option<String>,
}

/// The storage facts from which the shared definition-use authority judges.
///
/// Methods expose rows and closed existence facts, never a connection,
/// arbitrary query surface, concrete host, or pre-decided selection.
pub(crate) trait DefinitionCatalog {
    fn namespaces(&self) -> Result<Vec<NamespaceFact>>;
    fn grounding_closure(&self, namespace_id: i64) -> Result<Vec<(i64, i64)>>;
    fn session_enlists(&self, namespace_id: i64) -> Result<Vec<i64>>;
    fn lexical_imports(&self, owner: ImportOwner) -> Result<Vec<i64>>;
    fn exposures(&self) -> Result<Vec<(i64, i64)>>;
    fn session_aliases(&self) -> Result<Vec<(String, i64)>>;
    fn load_aliases(&self, owner: ImportOwner) -> Result<Vec<(String, i64)>>;
    fn candidates(&self, canonical: &str, namespace_ids: &[i64]) -> Result<Vec<CandidateFact>>;
    /// Every edge entity activated in one of `namespace_ids`.
    fn edge_declarations(&self, namespace_ids: &[i64]) -> Result<Vec<EdgeDeclarationFact>>;
    fn namespace_backing(&self, key: NamespaceKey<'_>) -> Result<Option<BackingFact>>;
    /// Every namespace whose mount binding runs on the connection.
    fn connection_bindings(&self, connection_id: i64) -> Result<Vec<String>>;
    /// Every session object on the connection holding the physical name,
    /// under the engine's ASCII case folding.
    fn session_holders(&self, connection_id: i64, name: &str) -> Result<Vec<SessionHolderFact>>;
    /// Session objects under the canonical name whose recorded durable owner
    /// is one of `durable_ids`, each with that owner's id.
    fn overlay_candidates(
        &self,
        canonical: &str,
        durable_ids: &[i64],
    ) -> Result<Vec<(i64, CandidateFact)>>;
    /// The session shadow a connection's temp schema is registered under,
    /// once its first session object fixed it.
    fn recorded_shadow(&self, connection_id: i64) -> Result<Option<String>>;
    /// The namespace an entity is activated in and the load it was
    /// activated under, read from its one activation row.
    fn activation(&self, entity_id: i64) -> Result<Option<(String, i64)>>;
    /// Every recorded mention of a name (folded) by another entity: the
    /// mentioning entity, its name, its namespace, and the qualifier as
    /// written (`None` for a bare mention).
    fn mentions_of(&self, canonical: &str, except: i64) -> Result<Vec<MentionFact>>;
    /// Every recorded qualified mention: the mentioning entity, its name,
    /// its namespace, and the qualifier as written.
    fn qualified_mentions(&self) -> Result<Vec<MentionFact>>;
}

/// One recorded mention of a name by a definition.
#[derive(Debug, Clone)]
pub(crate) struct MentionFact {
    pub(crate) entity_id: i64,
    pub(crate) entity: String,
    pub(crate) namespace: String,
    pub(crate) name: String,
    pub(crate) qualifier: Option<String>,
}

mod sqlite {
    use super::*;
    use crate::diagnostic::Runtime;
    use rusqlite::{Connection, OptionalExtension};
    use std::sync::MutexGuard;

    fn catalog_error(
        context: &'static str,
        error: impl std::fmt::Display,
    ) -> crate::DelightQLError {
        Runtime::catalog(context, error.to_string())
    }

    /// An entity row's kind, decoded where the row is read.
    fn entity_kind(row: &rusqlite::Row<'_>, at: usize) -> rusqlite::Result<crate::enums::EntityType> {
        let code: i32 = row.get(at)?;
        crate::enums::EntityType::from_i32(code)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(at, i64::from(code)))
    }

    fn id_list(ids: &[i64]) -> String {
        if ids.is_empty() {
            "(NULL)".to_string()
        } else {
            format!(
                "({})",
                ids.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
    }

    impl DefinitionCatalog for Connection {
        fn namespaces(&self) -> Result<Vec<NamespaceFact>> {
            let mut statement = self
                .prepare("SELECT id, fq_name, kind, default_data_ns FROM namespace")
                .map_err(|error| catalog_error("prepare definition namespace facts", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                })
                .map_err(|error| catalog_error("read definition namespace facts", error))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| catalog_error("decode definition namespace facts", error))?;
            rows.into_iter()
                .map(|(id, fq, kind, default_data_ns)| {
                    let kind =
                        NamespaceKind::decode(fq.as_deref().unwrap_or_default(), kind.as_deref())?;
                    Ok(NamespaceFact {
                        id,
                        fq,
                        kind,
                        default_data_ns,
                    })
                })
                .collect()
        }

        fn grounding_closure(&self, namespace_id: i64) -> Result<Vec<(i64, i64)>> {
            let mut statement = self
                .prepare(
                    "SELECT lib_namespace_id, grounded_namespace_id FROM grounding
                     WHERE root_namespace_id = (SELECT root_namespace_id FROM grounding
                                                WHERE grounded_namespace_id = ?1)",
                )
                .map_err(|error| catalog_error("prepare grounding closure facts", error))?;
            let rows = statement
                .query_map([namespace_id], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|error| catalog_error("read grounding closure facts", error))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| catalog_error("decode grounding closure facts", error))
        }

        fn session_enlists(&self, namespace_id: i64) -> Result<Vec<i64>> {
            let mut statement = self
                .prepare(
                    "SELECT from_namespace_id FROM enlisted_namespace
                     WHERE to_namespace_id = ?1",
                )
                .map_err(|error| catalog_error("prepare session enlist facts", error))?;
            let rows = statement
                .query_map([namespace_id], |row| row.get(0))
                .map_err(|error| catalog_error("read session enlist facts", error))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| catalog_error("decode session enlist facts", error))
        }

        fn lexical_imports(&self, owner: ImportOwner) -> Result<Vec<i64>> {
            let (column, id) = match owner {
                ImportOwner::Namespace(id) => ("namespace_id", id),
                ImportOwner::Cartridge(id) => ("cartridge_id", id),
            };
            let sql =
                format!("SELECT imported_namespace_id FROM lexical_import WHERE {column} = ?1");
            let mut statement = self
                .prepare(&sql)
                .map_err(|error| catalog_error("prepare lexical import facts", error))?;
            let rows = statement
                .query_map([id], |row| row.get(0))
                .map_err(|error| catalog_error("read lexical import facts", error))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| catalog_error("decode lexical import facts", error))
        }

        fn exposures(&self) -> Result<Vec<(i64, i64)>> {
            let mut statement = self
                .prepare(
                    "SELECT exposing_namespace_id, exposed_namespace_id FROM exposed_namespace",
                )
                .map_err(|error| catalog_error("prepare exposure facts", error))?;
            let rows = statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|error| catalog_error("read exposure facts", error))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| catalog_error("decode exposure facts", error))
        }

        fn session_aliases(&self) -> Result<Vec<(String, i64)>> {
            let mut statement = self
                .prepare("SELECT alias, target_namespace_id FROM namespace_alias")
                .map_err(|error| catalog_error("prepare session alias facts", error))?;
            let rows = statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|error| catalog_error("read session alias facts", error))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| catalog_error("decode session alias facts", error))
        }

        fn load_aliases(&self, owner: ImportOwner) -> Result<Vec<(String, i64)>> {
            let (column, id) = match owner {
                ImportOwner::Namespace(id) => ("namespace_id", id),
                ImportOwner::Cartridge(id) => ("cartridge_id", id),
            };
            let sql = format!(
                "SELECT alias, target_namespace_id FROM namespace_local_alias WHERE {column} = ?1"
            );
            let mut statement = self
                .prepare(&sql)
                .map_err(|error| catalog_error("prepare local alias facts", error))?;
            let rows = statement
                .query_map([id], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|error| catalog_error("read local alias facts", error))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| catalog_error("decode local alias facts", error))
        }

        fn candidates(&self, canonical: &str, namespace_ids: &[i64]) -> Result<Vec<CandidateFact>> {
            let sql = format!(
                "SELECT e.id, e.name, e.name_stropped, e.type,
                        (SELECT GROUP_CONCAT(ec.definition, char(10))
                         FROM (SELECT definition FROM entity_clause
                               WHERE entity_id = e.id ORDER BY ordinal) ec),
                        n.fq_name, c.source_uri, e.cartridge_id, n.id
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 LEFT JOIN cartridge c ON c.id = e.cartridge_id
                 WHERE (CASE WHEN e.name_stropped = 1 THEN e.name ELSE lower(e.name) END) = ?1
                   AND ae.namespace_id IN {}
                 ORDER BY n.fq_name, e.id",
                id_list(namespace_ids)
            );
            let mut statement = self
                .prepare(&sql)
                .map_err(|error| catalog_error("prepare definition candidate facts", error))?;
            let rows = statement
                .query_map([canonical], |row| {
                    Ok(CandidateFact {
                        entity_id: row.get(0)?,
                        name: row.get(1)?,
                        stropped: row.get(2)?,
                        kind: entity_kind(row, 3)?,
                        definition: row.get(4)?,
                        namespace: row.get(5)?,
                        source_uri: row.get(6)?,
                        cartridge_id: row.get(7)?,
                        namespace_id: row.get(8)?,
                    })
                })
                .map_err(|error| catalog_error("read definition candidate facts", error))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| catalog_error("decode definition candidate facts", error))
        }

        fn edge_declarations(&self, namespace_ids: &[i64]) -> Result<Vec<EdgeDeclarationFact>> {
            let sql = format!(
                "SELECT e.id, e.name, e.name_stropped, e.type,
                        (SELECT GROUP_CONCAT(ec.definition, char(10))
                         FROM (SELECT definition FROM entity_clause
                               WHERE entity_id = e.id ORDER BY ordinal) ec),
                        n.fq_name, c.source_uri, e.cartridge_id, n.id,
                        je.left_spelling, je.right_spelling, je.context_name
                 FROM join_edge je
                 JOIN entity e ON e.id = je.entity_id
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 LEFT JOIN cartridge c ON c.id = e.cartridge_id
                 WHERE je.clause_ordinal = 1 AND ae.namespace_id IN {}
                 ORDER BY n.fq_name, e.id",
                id_list(namespace_ids)
            );
            let mut statement = self
                .prepare(&sql)
                .map_err(|error| catalog_error("prepare edge facts", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok(EdgeDeclarationFact {
                        candidate: CandidateFact {
                            entity_id: row.get(0)?,
                            name: row.get(1)?,
                            stropped: row.get(2)?,
                            kind: entity_kind(row, 3)?,
                            definition: row.get(4)?,
                            namespace: row.get(5)?,
                            source_uri: row.get(6)?,
                            cartridge_id: row.get(7)?,
                            namespace_id: row.get(8)?,
                        },
                        left: row.get(9)?,
                        right: row.get(10)?,
                        context: row.get(11)?,
                    })
                })
                .map_err(|error| catalog_error("read edge facts", error))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| catalog_error("decode edge facts", error))
        }

        fn namespace_backing(&self, key: NamespaceKey<'_>) -> Result<Option<BackingFact>> {
            let (predicate, value): (&str, rusqlite::types::Value) = match key {
                NamespaceKey::Id(id) => ("n.id = ?1", id.into()),
                NamespaceKey::Fq(fq) => ("n.fq_name = ?1", fq.to_string().into()),
            };
            // `mount.namespace_id` is the binding's key, so the row is one.
            let sql = format!(
                "SELECT n.id, n.fq_name, n.kind,
                        m.class, m.qualification, m.attach_alias, m.engine_schema,
                        c.source_uri, c.connection_id, co.connection_type
                 FROM namespace n
                 LEFT JOIN mount m ON m.namespace_id = n.id
                 LEFT JOIN cartridge c ON c.id = m.cartridge_id
                 LEFT JOIN connection co ON co.id = c.connection_id
                 WHERE {predicate}"
            );
            let row = self
                .query_row(&sql, [value], |row| {
                    let class: Option<String> = row.get(3)?;
                    let mount = match class {
                        Some(class) => Some(MountFact {
                            class,
                            qualification: row.get(4)?,
                            attach_alias: row.get(5)?,
                            engine_schema: row.get(6)?,
                            source_uri: row.get(7)?,
                            connection_id: row.get(8)?,
                            connection_type: row.get(9)?,
                        }),
                        None => None,
                    };
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        mount,
                    ))
                })
                .optional()
                .map_err(|error| catalog_error("read namespace backing fact", error))?;
            row.map(|(namespace_id, fq, kind, mount)| {
                Ok(BackingFact {
                    kind: NamespaceKind::decode(&fq, kind.as_deref())?,
                    namespace_id,
                    fq,
                    mount,
                })
            })
            .transpose()
        }

        fn connection_bindings(&self, connection_id: i64) -> Result<Vec<String>> {
            let mut statement = self
                .prepare(
                    "SELECT n.fq_name FROM mount m
                     JOIN cartridge c ON c.id = m.cartridge_id
                     JOIN namespace n ON n.id = m.namespace_id
                     WHERE c.connection_id = ?1
                     ORDER BY n.fq_name",
                )
                .map_err(|error| catalog_error("prepare connection binding facts", error))?;
            let rows = statement
                .query_map([connection_id], |row| row.get(0))
                .map_err(|error| catalog_error("read connection binding facts", error))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| catalog_error("decode connection binding facts", error))
        }

        fn session_holders(
            &self,
            connection_id: i64,
            name: &str,
        ) -> Result<Vec<SessionHolderFact>> {
            let mut statement = self
                .prepare(
                    "SELECT e.type, owner.fq_name
                     FROM session_overlay so
                     JOIN entity e ON e.id = so.entity_id
                     JOIN cartridge c ON c.id = e.cartridge_id
                     JOIN activated_entity ae ON ae.entity_id = e.id
                     JOIN namespace n ON n.id = ae.namespace_id
                     LEFT JOIN namespace owner ON owner.id = so.durable_namespace_id
                     WHERE c.connection_id = ?1 AND e.name = ?2 COLLATE NOCASE
                     ORDER BY e.id",
                )
                .map_err(|error| catalog_error("prepare session holder facts", error))?;
            let rows = statement
                .query_map(rusqlite::params![connection_id, name], |row| {
                    Ok(SessionHolderFact {
                        kind: row.get(0)?,
                        owner: row.get(1)?,
                    })
                })
                .map_err(|error| catalog_error("read session holder facts", error))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| catalog_error("decode session holder facts", error))
        }

        fn overlay_candidates(
            &self,
            canonical: &str,
            durable_ids: &[i64],
        ) -> Result<Vec<(i64, CandidateFact)>> {
            let sql = format!(
                "SELECT so.durable_namespace_id, e.id, e.name, e.name_stropped, e.type,
                        (SELECT GROUP_CONCAT(ec.definition, char(10))
                         FROM (SELECT definition FROM entity_clause
                               WHERE entity_id = e.id ORDER BY ordinal) ec),
                        n.fq_name, c.source_uri, e.cartridge_id, n.id
                 FROM session_overlay so
                 JOIN entity e ON e.id = so.entity_id
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 LEFT JOIN cartridge c ON c.id = e.cartridge_id
                 WHERE (CASE WHEN e.name_stropped = 1 THEN e.name ELSE lower(e.name) END) = ?1
                   AND so.durable_namespace_id IN {}
                 ORDER BY n.fq_name, e.id",
                id_list(durable_ids)
            );
            let mut statement = self
                .prepare(&sql)
                .map_err(|error| catalog_error("prepare session overlay facts", error))?;
            let rows = statement
                .query_map([canonical], |row| {
                    Ok((
                        row.get(0)?,
                        CandidateFact {
                            entity_id: row.get(1)?,
                            name: row.get(2)?,
                            stropped: row.get(3)?,
                            kind: entity_kind(row, 4)?,
                            definition: row.get(5)?,
                            namespace: row.get(6)?,
                            source_uri: row.get(7)?,
                            cartridge_id: row.get(8)?,
                            namespace_id: row.get(9)?,
                        },
                    ))
                })
                .map_err(|error| catalog_error("read session overlay facts", error))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| catalog_error("decode session overlay facts", error))
        }

        fn recorded_shadow(&self, connection_id: i64) -> Result<Option<String>> {
            self.query_row(
                "SELECT n.fq_name FROM connection co
                 JOIN namespace n ON n.id = co.shadow_namespace_id
                 WHERE co.id = ?1",
                [connection_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| catalog_error("read recorded shadow fact", error))
        }

        fn activation(&self, entity_id: i64) -> Result<Option<(String, i64)>> {
            self.query_row(
                "SELECT n.fq_name, ae.cartridge_id
                 FROM activated_entity ae
                 JOIN namespace n ON n.id = ae.namespace_id
                 WHERE ae.entity_id = ?1",
                [entity_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| catalog_error("read an entity's activation", error))
        }

        fn mentions_of(&self, canonical: &str, except: i64) -> Result<Vec<MentionFact>> {
            let mut statement = self
                .prepare(
                    "SELECT e.id, e.name, n.fq_name, re.name, re.namespace
                     FROM referenced_entity re
                     JOIN entity e ON e.id = re.containing_entity_id
                     JOIN activated_entity ae ON ae.entity_id = e.id
                     JOIN namespace n ON n.id = ae.namespace_id
                     WHERE lower(re.name) = ?1 AND e.id <> ?2
                     ORDER BY n.fq_name, e.name, re.id",
                )
                .map_err(|error| catalog_error("prepare mention facts", error))?;
            let rows = statement
                .query_map(rusqlite::params![canonical, except], |row| {
                    Ok(MentionFact {
                        entity_id: row.get(0)?,
                        entity: row.get(1)?,
                        namespace: row.get(2)?,
                        name: row.get(3)?,
                        qualifier: row.get(4)?,
                    })
                })
                .map_err(|error| catalog_error("read mention facts", error))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| catalog_error("decode mention facts", error))
        }

        fn qualified_mentions(&self) -> Result<Vec<MentionFact>> {
            let mut statement = self
                .prepare(
                    "SELECT e.id, e.name, n.fq_name, re.name, re.namespace
                     FROM referenced_entity re
                     JOIN entity e ON e.id = re.containing_entity_id
                     JOIN activated_entity ae ON ae.entity_id = e.id
                     JOIN namespace n ON n.id = ae.namespace_id
                     WHERE re.namespace IS NOT NULL
                     ORDER BY n.fq_name, e.name, re.id",
                )
                .map_err(|error| catalog_error("prepare qualified mention facts", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok(MentionFact {
                        entity_id: row.get(0)?,
                        entity: row.get(1)?,
                        namespace: row.get(2)?,
                        name: row.get(3)?,
                        qualifier: row.get(4)?,
                    })
                })
                .map_err(|error| catalog_error("read qualified mention facts", error))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| catalog_error("decode qualified mention facts", error))
        }
    }

    pub(crate) struct LockedDefinitionCatalog<'a> {
        guard: MutexGuard<'a, Connection>,
    }

    impl<'a> LockedDefinitionCatalog<'a> {
        pub(crate) fn new(guard: MutexGuard<'a, Connection>) -> Self {
            Self { guard }
        }
    }

    macro_rules! delegate {
        ($name:ident ( $($arg:ident : $type:ty),* ) -> $result:ty) => {
            fn $name(&self, $($arg: $type),*) -> Result<$result> {
                DefinitionCatalog::$name(&*self.guard, $($arg),*)
            }
        };
    }

    impl DefinitionCatalog for LockedDefinitionCatalog<'_> {
        delegate!(namespaces() -> Vec<NamespaceFact>);
        delegate!(grounding_closure(namespace_id: i64) -> Vec<(i64, i64)>);
        delegate!(session_enlists(namespace_id: i64) -> Vec<i64>);
        delegate!(lexical_imports(owner: ImportOwner) -> Vec<i64>);
        delegate!(exposures() -> Vec<(i64, i64)>);
        delegate!(session_aliases() -> Vec<(String, i64)>);
        delegate!(load_aliases(owner: ImportOwner) -> Vec<(String, i64)>);
        delegate!(candidates(canonical: &str, namespace_ids: &[i64]) -> Vec<CandidateFact>);
        delegate!(edge_declarations(namespace_ids: &[i64]) -> Vec<EdgeDeclarationFact>);
        delegate!(namespace_backing(key: NamespaceKey<'_>) -> Option<BackingFact>);
        delegate!(connection_bindings(connection_id: i64) -> Vec<String>);
        delegate!(session_holders(connection_id: i64, name: &str) -> Vec<SessionHolderFact>);
        delegate!(overlay_candidates(canonical: &str, durable_ids: &[i64]) -> Vec<(i64, CandidateFact)>);
        delegate!(recorded_shadow(connection_id: i64) -> Option<String>);
        delegate!(activation(entity_id: i64) -> Option<(String, i64)>);
        delegate!(mentions_of(canonical: &str, except: i64) -> Vec<MentionFact>);
        delegate!(qualified_mentions() -> Vec<MentionFact>);
    }
}

pub(crate) use sqlite::LockedDefinitionCatalog;
