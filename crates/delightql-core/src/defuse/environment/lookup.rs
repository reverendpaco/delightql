// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE ONE RELATION LOOKUP. Both worlds answer an unqualified relation name
//! through this ladder — locals, then the captured reach, then the data
//! binding — and answer with a CLOSED result. Position-specific
//! admissibility maps the answer to its artifact; it never performs another
//! name search.

use crate::error::{DelightQLError, Result};
use crate::resolution::{
    EntityDefinition, EntityInfo, RegistrySource, ResolvedEntityKind, ResolverCore, SchemaSource,
};
use delightql_types::SqlIdentifier;

use super::{Environment, QueryLocalSelection};

/// The closed answer to "what does this relation name denote here?".
#[derive(Debug)]
pub(crate) enum RelationAnswer<'s> {
    /// A known built-in function answers the name; relation position
    /// treats it as its own miss.
    BuiltInFunction,
    /// Database entity (table, view, etc.)
    DatabaseEntity(EntityInfo),
    /// A CTE this world owns
    CTE {
        entity: EntityInfo,
        frontier: Option<crate::defuse::instance::DefinitionFrontier>,
    },
    /// A physical relation created by an earlier statement in the same plan.
    ///
    /// Its heading is lexical knowledge, but unlike a CTE it may be a DML
    /// target.
    MaterializedRelation(EntityInfo),
    /// Consulted definition with a relational body — needs body expansion at
    /// the relation level. Facts take this road too.
    ///
    /// The FAMILY travels whole: its body opens through it, in the
    /// declaration environment it names.
    ConsultedView(crate::defuse::bound_use::SelectedRelation<'s>),
    /// A consulted entity whose name resolved, but whose kind cannot occupy
    /// relation position. Relation resolution retains the kind so it can teach
    /// the valid invocation form instead of claiming the name is absent.
    DefinedNonRelation {
        name: SqlIdentifier,
        entity_type: crate::enums::EntityType,
    },
    /// A RELATION THE RUNTIME SERVES. Its category is relational but no
    /// schema this resolver can consult holds its rows.
    RuntimeServedRelation {
        name: SqlIdentifier,
        entity_type: crate::enums::EntityType,
    },
    /// A FREE DATA NAME of a declaration whose world no `ground!` bound. It
    /// is not missing — it is a hole nothing may fill ambiently.
    DataHole { name: SqlIdentifier, world: String },
    /// Several relation-capable candidates answer the name in this reach.
    Ambiguous(String),
    /// Unknown name — the position's own miss teaching answers it.
    Unknown,
}

/// The relation-position capability judgment over one candidate set: a
/// value callable cannot stand in relation position, so it never perturbs
/// relation selection and is kept only as the kind teaching when nothing
/// relation-capable answers.
enum Judged<'s> {
    Answered(crate::defuse::select::Selected<'s>),
    /// EVERY wrong-kind candidate, never a scan-order survivor: one is the
    /// kind teaching, several are an ambiguity with full provenance.
    WrongKind(Vec<crate::defuse::select::Enumerated<'s>>),
    Ambiguous(Vec<crate::defuse::select::Candidate>),
    Missing,
}

fn judge_relation_candidates<'s>(
    candidates: Vec<crate::defuse::select::Enumerated<'s>>,
) -> Judged<'s> {
    use crate::enums::EntityType;
    let (mut capable, unfit): (Vec<_>, Vec<_>) = candidates
        .into_iter()
        .partition(|candidate| candidate.kind().realizes_relation());
    // TEMP SHADOWS PERMANENT (engine semantics): one namespace serving the
    // same unqualified name from its temp schema and its durable one is a
    // resolution preference, not ambiguity — the engine reads the temp, and
    // the qualified spelling still reaches the durable table.
    if capable.len() == 2
        && capable[0].namespace() == capable[1].namespace()
        && capable
            .iter()
            .any(|candidate| candidate.kind() == EntityType::DbTemporaryTable)
        && capable
            .iter()
            .any(|candidate| candidate.kind() == EntityType::DbPermanentTable)
    {
        capable.retain(|candidate| candidate.kind() == EntityType::DbTemporaryTable);
    }
    // THE BOTH-FACES COLLISION LAW, relation side: an AUTHORED sigma rule
    // standing beside a live relation-capable answer refuses — the same
    // refusal the sigma position applies while both definitions are live
    // (the collision_policy family). Only the authored sigma kind pairs;
    // bin predicates and other wrong kinds perturb nothing.
    if !capable.is_empty()
        && unfit
            .iter()
            .any(|candidate| candidate.kind() == EntityType::DqlTemporarySigmaRule)
    {
        let listed: Vec<_> = capable
            .into_iter()
            .chain(
                unfit
                    .into_iter()
                    .filter(|candidate| candidate.kind() == EntityType::DqlTemporarySigmaRule),
            )
            .map(|candidate| candidate.into_provenance())
            .collect();
        return Judged::Ambiguous(listed);
    }
    match capable.len() {
        0 => {
            if unfit.is_empty() {
                Judged::Missing
            } else {
                Judged::WrongKind(unfit)
            }
        }
        1 => Judged::Answered(
            capable
                .into_iter()
                .next()
                .expect("len checked")
                .into_selected(),
        ),
        _ => Judged::Ambiguous(
            capable
                .into_iter()
                .map(|candidate| candidate.into_provenance())
                .collect(),
        ),
    }
}

impl Environment {
    /// A query alias resolves once and is remembered for the rest of the
    /// query, so later mentions of it answer the same relation.
    fn note_alias(&mut self, alias: Option<&SqlIdentifier>, actual: &SqlIdentifier) {
        if let Some(alias_name) = alias {
            if alias_name != actual {
                self.register_alias(alias_name.clone(), actual.clone());
            }
        }
    }

    /// Answer an unqualified relation name in THIS world.
    ///
    /// The ladder: a query alias, the world's own CTEs, its plan-created
    /// relations, the built-in functions, then the captured reach (the
    /// authored families its namespaces currently publish beside the served
    /// relations of the reachable data namespaces), then the data binding.
    /// A miss past all
    /// of that is a data hole inside a declaration and an unknown name at
    /// the prompt; it never falls through to any other world.
    pub(crate) fn relation<'db>(
        &mut self,
        core: &mut ResolverCore<'db>,
        name: &SqlIdentifier,
        alias: Option<&SqlIdentifier>,
    ) -> Result<RelationAnswer<'db>> {
        let actual_name = self.alias_target(name).unwrap_or_else(|| name.clone());

        if let Some(QueryLocalSelection::Relation(cte)) = self.select_query_local(
            &actual_name,
            crate::pipeline::asts::core::QueryLocalDemand::Relation,
            None,
        )? {
            let relation = cte.relation();
            self.note_alias(alias, &actual_name);
            return Ok(RelationAnswer::CTE {
                entity: local_relation_entity(actual_name.clone(), None, relation),
                frontier: cte.frontier(),
            });
        }

        if let Some(relation) = self.materialized(&actual_name) {
            self.note_alias(alias, &actual_name);
            return Ok(RelationAnswer::MaterializedRelation(local_relation_entity(
                actual_name.clone(),
                Some(actual_name.clone()),
                relation,
            )));
        }

        if core.built_in.is_known_function(actual_name.as_str()) {
            return Ok(RelationAnswer::BuiltInFunction);
        }

        let Some(catalog) = core.consult.catalog() else {
            // No system, so no namespace to reach: the database catalog is
            // the only registry there is (unit tests build one this way).
            return Ok(match core.database.lookup_table(actual_name.as_str())? {
                Some(schema) => RelationAnswer::DatabaseEntity(catalog_entity(
                    actual_name,
                    None,
                    None,
                    None,
                    schema,
                )),
                None => RelationAnswer::Unknown,
            });
        };

        // THE ONE LEXICAL-LINK / DATA-HOLE JUDGMENT, under the read's lock:
        // the captured reach answers first (the families currently
        // published in this world's namespaces, beside the served relations
        // activated in its reachable data namespaces); only a reach miss is
        // a data hole, which the world an explicit grounding published (for
        // this namespace or for the grounded closure this body derives
        // under), or a scratch world's own ambient data, may fill — and no
        // other. Grounding admission consumes this same judgment.
        let link = {
            let conn =
                catalog.connection("Failed to acquire bootstrap lock for relation lookup")?;
            crate::defuse::select::judge_link_on(
                &conn,
                catalog,
                self.reach(),
                self.data().data_ns(),
                actual_name.as_str(),
                actual_name.is_stropped(),
            )?
        };
        let hole = match link {
            crate::defuse::select::Link::Lexical(candidates) => {
                match judge_relation_candidates(candidates) {
                    Judged::Answered(selected) => {
                        return realize_relation(core, actual_name, selected);
                    }
                    Judged::WrongKind(unfit) => {
                        if unfit.len() > 1 {
                            // Several wrong-kind owners: report the COMPLETE
                            // set — a first match cannot establish uniqueness
                            // even when every candidate is incapable.
                            return Ok(RelationAnswer::Ambiguous(
                                crate::defuse::select::ambiguity_refusal(
                                    actual_name.as_str(),
                                    &unfit
                                        .into_iter()
                                        .map(|candidate| candidate.into_provenance())
                                        .collect::<Vec<_>>(),
                                )
                                .to_string(),
                            ));
                        }
                        let selected = unfit
                            .into_iter()
                            .next()
                            .expect("the wrong-kind arm holds at least one candidate")
                            .into_selected();
                        return Ok(RelationAnswer::DefinedNonRelation {
                            name: selected.name().clone(),
                            entity_type: selected.kind(),
                        });
                    }
                    Judged::Ambiguous(candidates) => {
                        return Ok(RelationAnswer::Ambiguous(
                            crate::defuse::select::ambiguity_refusal(
                                actual_name.as_str(),
                                &candidates,
                            )
                            .to_string(),
                        ));
                    }
                    Judged::Missing => None,
                }
            }
            crate::defuse::select::Link::Hole(selection) => selection,
        };

        // A non-authoritative system (WASM, pipe connections) cannot
        // enumerate its catalog, so the schema provider answers for it.
        if !catalog.system().namespace_authoritative {
            if let Some(table_schema) = core.database.lookup_table(actual_name.as_str())? {
                return Ok(RelationAnswer::DatabaseEntity(catalog_entity(
                    actual_name.clone(),
                    None,
                    None,
                    None,
                    table_schema,
                )));
            }
        }

        // THE DATA HOLE: filled by the bound data world's unique answer,
        // else a hole inside a declaration and an unknown name at the
        // prompt.
        match hole {
            Some(selection) => match selection.unique_or_refuse(actual_name.as_str())? {
                Some(selected) => realize_relation(core, actual_name, selected),
                None => Ok(self.data_miss(actual_name)),
            },
            None => Ok(self.data_miss(actual_name)),
        }
    }

    /// A miss past the whole ladder: a DATA HOLE inside a declaration —
    /// nothing may fill it ambiently — and an unknown name at the prompt.
    fn data_miss<'s>(&self, name: SqlIdentifier) -> RelationAnswer<'s> {
        if self.is_declaration() {
            RelationAnswer::DataHole {
                name,
                world: self.display_scope().to_string(),
            }
        } else {
            RelationAnswer::Unknown
        }
    }
}

/// A qualified catalog hit that the materialization law serves as a
/// LITERAL SNAPSHOT instead of a read: connection 1 is a source, never a
/// target, and exemption is absence from the attribution set.
pub(crate) struct BootstrapServe {
    pub(crate) canonical: delightql_types::SqlIdentifier,
    pub(crate) backend_schema: Option<String>,
    pub(crate) namespace_fq: String,
}

impl Environment {
    /// Answer a NAMESPACE-QUALIFIED relation name — the same closed
    /// authority as the unqualified ladder, owning the decision that
    /// licenses reaching the late database provider: the catalog first
    /// (locals cannot carry namespace paths), then the definition catalog
    /// for the named namespace as this statement's read holds it, then a
    /// closed miss. Nothing outside this method searches again.
    pub(crate) fn relation_qualified<'db>(
        &self,
        core: &mut ResolverCore<'db>,
        path: &crate::pipeline::ast_resolved::NamespacePath,
        name: &SqlIdentifier,
        serve_bootstrap_reads: bool,
    ) -> Result<(RelationAnswer<'db>, Option<BootstrapServe>)> {
        // The qualifier may be an ALIAS this world declared: the reach
        // translates it BEFORE any provider is asked, so the alias and the
        // exact spelling take one road.
        let translated;
        let path = match self.reach().alias_target(&path.fq_string()) {
            Some(aliased) => {
                translated =
                    crate::pipeline::ast_resolved::NamespacePath::from_fq_string(&aliased.fq)
                        .map_err(|e| {
                            DelightQLError::database_error(
                                format!(
                                    "corrupt catalog: alias targets namespace '{}'",
                                    aliased.fq
                                ),
                                format!("{e:?}"),
                            )
                        })?;
                &translated
            }
            None => path,
        };
        match core
            .database
            .lookup_table_with_namespace_qualified(path, name)
        {
            Ok(Some((table_schema, connection_id, canonical_name, backend_schema))) => {
                // THE BOOTSTRAP IS A SOURCE, NEVER A TARGET
                // (materialization-law §2): while a materialization source
                // resolves, a bootstrap read is answered as a literal
                // snapshot, and connection 1 never enters the attribution
                // set — exemption is ABSENCE, not a tie-break.
                let mut serve = None;
                if connection_id == 1 && serve_bootstrap_reads {
                    serve = Some(BootstrapServe {
                        canonical: canonical_name.clone(),
                        backend_schema: backend_schema.clone(),
                        namespace_fq: path.fq_string(),
                    });
                } else {
                    core.track_connection_id(connection_id);
                }
                let entity = catalog_entity(
                    name.clone(),
                    Some(canonical_name),
                    Some(path.clone()),
                    backend_schema,
                    table_schema,
                );
                Ok((RelationAnswer::DatabaseEntity(entity), serve))
            }
            Ok(None) => {
                // Not a database table — the definition catalog answers for
                // the named namespace.
                let fq = path.fq_string();
                let answer = match crate::defuse::bound_use::classify_relation(
                    core,
                    self.reach(),
                    name.as_str(),
                    name.is_stropped(),
                    &fq,
                )? {
                    Some(classified) => classified,
                    None => RelationAnswer::Unknown,
                };
                Ok((answer, None))
            }
            // Namespace resolution failed (unknown namespace): a closed
            // refusal, never a fall-through to another road.
            Err(e) => Err(e),
        }
    }
}

/// Realize one selected candidate as the relation answer its kind admits.
fn realize_relation<'s>(
    core: &mut ResolverCore,
    actual_name: SqlIdentifier,
    selected: crate::defuse::select::Selected<'s>,
) -> Result<RelationAnswer<'s>> {
    use crate::enums::EntityType;
    match selected {
        crate::defuse::select::Selected::Authored(family) => {
            Ok(crate::defuse::bound_use::classify_family(family))
        }
        crate::defuse::select::Selected::Served(served) => match served.kind() {
            kind @ EntityType::BinRelation => Ok(RelationAnswer::RuntimeServedRelation {
                name: served.name().clone(),
                entity_type: kind,
            }),
            EntityType::BinPseudoPredicate
            | EntityType::BinSigmaPredicate
            | EntityType::SyntaxDirective => Ok(RelationAnswer::DefinedNonRelation {
                name: served.name().clone(),
                entity_type: served.kind(),
            }),
            _ => {
                let namespace_path = crate::pipeline::ast_resolved::NamespacePath::from_fq_string(
                    served.namespace(),
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "corrupt catalog: served entity '{}' names namespace '{}'",
                            served.name(),
                            served.namespace()
                        ),
                        format!("{e:?}"),
                    )
                })?;
                match core
                    .database
                    .lookup_table_with_namespace(&namespace_path, actual_name.as_str())?
                {
                    Some((table_schema, connection_id, _registry_canonical, backend_schema)) => {
                        core.track_connection_id(connection_id);
                        Ok(RelationAnswer::DatabaseEntity(catalog_entity(
                            actual_name,
                            Some(served.name().clone()),
                            Some(namespace_path),
                            backend_schema,
                            table_schema,
                        )))
                    }
                    None => Ok(RelationAnswer::Unknown),
                }
            }
        },
    }
}

/// The entity record a world-local relation (CTE, plan materialization)
/// answers with: its schema is its registration.
fn local_relation_entity(
    name: SqlIdentifier,
    canonical_name: Option<SqlIdentifier>,
    relation: crate::relation::SemanticRelation,
) -> EntityInfo {
    EntityInfo {
        name,
        canonical_name,
        resolved_namespace: None,
        backend_schema: None,
        entity_type: ResolvedEntityKind::Relation,
        registry_source: RegistrySource::QueryLocal,
        schema_source: SchemaSource::SelectClause,
        definition: EntityDefinition::RelationSchema(relation),
    }
}

fn catalog_entity(
    name: SqlIdentifier,
    canonical_name: Option<SqlIdentifier>,
    resolved_namespace: Option<crate::pipeline::ast_resolved::NamespacePath>,
    backend_schema: Option<String>,
    relation: crate::relation::SemanticRelation,
) -> EntityInfo {
    EntityInfo {
        name,
        canonical_name,
        resolved_namespace,
        backend_schema,
        entity_type: ResolvedEntityKind::Relation,
        registry_source: RegistrySource::Database,
        schema_source: SchemaSource::DatabaseCatalog,
        definition: EntityDefinition::RelationSchema(relation),
    }
}

/// The refusal an unbound data hole produces where a relation was asked for.
pub(crate) fn unbound_data_hole(name: &SqlIdentifier, world: &str) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        crate::uri_registry::subcat::GROUNDING_DATA_HOLE_UNBOUND,
        format!(
            "'{name}' is a free data name of '{world}', and no ground! has bound that \
             world's data holes to a data world. A consulted body reads its own \
             definitions and the data world an explicit grounding published — never \
             the caller's tables, CTEs, or session database ambiently"
        ),
        "ground!(\"<data namespace>\", \"<rules namespace>\", \"<grounded name>\") binds the \
         holes, or enlist!(\"<data namespace>\") inside the consulted file links them",
    )
}

/// The SIGMA-POSITION judgment over ONE exhaustive enumeration: the same
/// candidate set yields both faces — the sigma rule and the relation
/// capability — so probe order and erased ambiguities can never invent a
/// collision. A wrong-kind crowd is NEITHER face.
pub(in crate::defuse) enum SigmaPosition<'s> {
    /// A sigma rule AND a relation-capable candidate both answer: the
    /// position's own collision refusal, with the sigma's provenance.
    Collision {
        sigma: crate::defuse::select::LinkedFamily<'s>,
    },
    /// Exactly the sigma answers.
    Sigma(crate::defuse::select::LinkedFamily<'s>),
    /// A relation face answers and no sigma does.
    RelationAnswers,
    /// Neither face answers.
    Neither,
}

impl Environment {
    /// Judge an unqualified name in SIGMA (existence) position: locals
    /// first (a CTE or plan relation IS the relation face), then one
    /// enumeration of the world's reach partitioned by capability.
    pub(in crate::defuse) fn sigma_position<'db>(
        &self,
        core: &mut ResolverCore<'db>,
        name: &SqlIdentifier,
    ) -> Result<SigmaPosition<'db>> {
        let mut relation_face = matches!(
            self.select_query_local(
                name,
                crate::pipeline::asts::core::QueryLocalDemand::Relation,
                None,
            )?,
            Some(QueryLocalSelection::Relation(_))
        ) || self.materialized(name).is_some();
        let mut sigmas: Vec<crate::defuse::select::Enumerated<'db>> = Vec::new();
        if let Some(catalog) = core.consult.catalog() {
            let candidates = crate::defuse::select::enumerate_in_reach(
                catalog,
                name.as_str(),
                name.is_stropped(),
                self.reach(),
            )?;
            for candidate in candidates {
                if candidate.kind() == crate::enums::EntityType::DqlTemporarySigmaRule {
                    sigmas.push(candidate);
                } else if candidate.kind().realizes_relation() {
                    // The relation face, by the SAME positive capability
                    // judgment the relation ladder applies.
                    relation_face = true;
                }
                // Everything else — functions, bin predicates, effect and
                // ER rules, directives — is NEITHER face: it cannot make a
                // relation answer, and a same-named crowd of such kinds
                // proves nothing.
            }
        } else if core.database.lookup_table(name.as_str())?.is_some() {
            relation_face = true;
        }
        let sigma = match crate::defuse::select::judge_position(name.as_str(), sigmas, |kind| {
            kind == crate::enums::EntityType::DqlTemporarySigmaRule
        })? {
            crate::defuse::select::PositionOutcome::Selected(
                crate::defuse::select::Selected::Authored(family),
            ) => Some(family),
            _ => None,
        };
        Ok(match (sigma, relation_face) {
            (Some(sigma), true) => SigmaPosition::Collision { sigma },
            (Some(sigma), false) => SigmaPosition::Sigma(sigma),
            (None, true) => SigmaPosition::RelationAnswers,
            (None, false) => SigmaPosition::Neither,
        })
    }
}
