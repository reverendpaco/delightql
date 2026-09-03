// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Relation resolution logic
//!
//! This module handles the resolution of base relations and relational calls.
//! and pattern application for positional patterns.
use super::ResolvedRelation;
use crate::pipeline::asts::core::{AuthoredColumn, ColumnOccurrence, GroundForm};

use super::tvf::get_tvf_schema;
use super::type_conversion::{convert_domain_expression, convert_qualified_name};
use crate::enums::EntityType as BootstrapEntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_resolved::NamespacePath;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::core::{NamedReference, Reference};
use delightql_types::SqlIdentifier;

pub(super) fn bind_physical_relation(
    relation: crate::relation::SemanticRelation,
    canonical: Option<&SqlIdentifier>,
    backend_schema: Option<&str>,
    identities: &crate::relation::Planning,
) -> Result<()> {
    let Some(entity) = identities.authority().entity(&relation)? else {
        return Err(DelightQLError::parse_error(
            "A physical relation heading has no catalog entity identity",
        ));
    };
    let canonical = canonical.map(|name| identities.intern(name.as_str(), name.is_stropped()));
    let backend_schema = backend_schema.map(|name| identities.intern(name, false));
    identities.bind_entity_physical(entity, canonical, backend_schema);
    Ok(())
}

/// Resolve a relation-access shape that has no heading available for binding.
///
/// Glob shapes carry no slot expressions. A positional shape needs a source
/// heading so each authored slot can become the occurrence it binds.
pub(super) fn resolve_schema_free_access(
    spec: &ast_unresolved::Access,
) -> Result<ast_resolved::Access> {
    match spec {
        ast_unresolved::Access::All => Ok(ast_resolved::Access::All),
        ast_unresolved::Access::Unasked => Ok(ast_resolved::Access::Unasked),
        ast_unresolved::Access::Dequalify(columns) => {
            Ok(ast_resolved::Access::Dequalify(columns.clone()))
        }
        ast_unresolved::Access::DequalifyAll => Ok(ast_resolved::Access::DequalifyAll),
        ast_unresolved::Access::Slots(_) => Err(DelightQLError::validation_error(
            "A positional relation access requires a resolved heading",
            "Positional pattern resolution",
        )),
    }
}

/// The access-boundary export for a consulted view/fact.
///
/// A view's lvars are a function of how it is CALLED, not of how its body
/// spelled them, and the access name — the user's alias, or the bare view name
/// — is what qualifies them. But the name a column answers to and the name it
/// is published under are two facts, not one: exporting a column that answers
/// ONLY to the access name makes `v(*), name = "x"` unaddressable, because
/// nothing published `name` any more. So the column keeps its published
/// name, and whether the access name reaches it is the lexical frontier's
/// binding of the boundary relation — never a fact recorded on the column.
///
/// What still does not cross is the caller's own argumentative binding —
/// `declared_bare` belongs to the call site and never leaks through the entity
/// boundary. The SQL occurrence stays distinct (hygiene: self-joins need
/// distinct aliases), while the access name rides the metadata rather than
/// naming that occurrence.
/// Say where a consulted body failed WITHOUT taking its badge away.
///
pub(crate) fn access_boundary_answer(
    alias: &Option<SqlIdentifier>,
    entity_name: &SqlIdentifier,
    identities: &crate::relation::Planning,
) -> (SqlIdentifier, crate::names::Spelling) {
    let effective = match alias.clone() {
        Some(a) => a,
        None => entity_name.clone(),
    };
    let answer = identities.intern(effective.as_str(), effective.is_stropped());
    (effective, answer)
}

/// The owner of a mention's slot row: the alias the author wrote, or none.
fn pattern_owner(alias: &Option<SqlIdentifier>) -> super::PatternOwner {
    match alias {
        Some(alias) => super::PatternOwner::Authored(alias.clone()),
        None => super::PatternOwner::Unqualified,
    }
}

// resolve_relation_with_registry — DELETED (Step 0f). Dispatch absorbed into
// ResolverFold::resolve_relation_impl (resolver_fold.rs).

/// Relabel the published columns of a resolved relation with a new table name.
///
/// Consulted entities (facts and views) resolve their bodies internally, producing
/// columns with the entity's original table name. When the entity is aliased
/// (e.g., `country_tier(*) as ct`), downstream pipes need `i_provide` columns
/// to carry the alias so qualified refs like `ct.Country` can match.
/// A RESOLVED DEFINITION BODY AS THE RELATION A CALL READS. A body with
/// no CTEs is the relation itself, under the caller's alias when one was
/// written; a body with CTEs stands behind a consulted-view boundary the
/// authority mints, answering to the view's name or the alias. The
/// resolver's own product enters here; no identity does.
pub(crate) fn view_query_to_relational(
    resolved_query: crate::pipeline::resolver::ResolvedQuery,
    view_name: &str,
    user_alias: Option<SqlIdentifier>,
    identities: &crate::relation::Planning,
) -> Result<ResolvedRelation> {
    use crate::pipeline::asts::core::GroundForm;
    match resolved_query.into_relational_body() {
        Ok(resolved) => match user_alias {
            Some(alias) => {
                let spelling = identities.intern(alias.as_str(), alias.is_stropped());
                resolved.aliased(spelling, identities)
            }
            None => Ok(resolved),
        },
        Err(query_with_ctes) => {
            let query_with_ctes = query_with_ctes.into_query();
            let (_alias, answer) =
                access_boundary_answer(&user_alias, &SqlIdentifier::new(view_name), identities);
            let head = identities.authority().boundary_head(
                GroundForm::Reference(ast_resolved::Relation::ConsultedView {
                    body: Box::new(query_with_ctes),
                    outer: false,
                }),
                crate::relation::builder::Boundary::Instance {
                    kind: crate::relation::form::DefinitionKind::View,
                    answers_to: Some(answer),
                },
            )?;
            Ok(ResolvedRelation::answering_for_itself(
                ast_resolved::Chain::ground(head),
            ))
        }
    }
}

/// A STRUCTURAL MENTION: the body addresses one of its formals by the
/// landing its call bound, and the world the body was opened from answers
/// with the carrier — the record of the act that bound it holds the
/// relationship; nothing here pairs a landing with a relation.
pub(super) fn resolve_structural_scope(
    rel: ast_unresolved::Relation,
    access: ast_unresolved::Access,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
) -> Result<ResolvedRelation> {
    let ast_unresolved::Relation::Ground {
        mention:
            ast_unresolved::GroundMention::Structural {
                pending,
                authored_name,
                alias,
            },
        outer,
    } = rel
    else {
        unreachable!("resolve_structural_scope called with a different relation")
    };
    let carrier = fold.env.structural(pending).ok_or_else(|| {
        DelightQLError::validation_error(
            "A structural relation was read before its binding was resolved",
            "structural relation",
        )
    })?;
    read_compiler_relation(carrier, authored_name, alias, access, outer, fold)
}

/// A SCRATCH READ: a plan reads a row it allocated, by the receipt of the
/// allocation. Nothing authored stands on it, so it carries no access
/// metadata and answers for itself.
pub(super) fn resolve_scratch_read(
    rel: ast_unresolved::Relation,
    access: ast_unresolved::Access,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
) -> Result<ResolvedRelation> {
    let ast_unresolved::Relation::Ground {
        mention: ast_unresolved::GroundMention::Scratch { row },
        outer,
    } = rel
    else {
        unreachable!("resolve_scratch_read called with a different relation")
    };
    read_compiler_relation(
        crate::defuse::carriers::CompilerRow::scratch(row),
        None,
        None,
        access,
        outer,
        fold,
    )
}

/// A RECEIPT READ: a scratch row standing where the author wrote a name,
/// paired with that name by the plan that placed it there. The read is an
/// authored access under the name — the scratch object in the inner FROM
/// and the caller-facing occurrence outside it.
pub(super) fn resolve_receipt_read(
    rel: ast_unresolved::Relation,
    access: ast_unresolved::Access,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
) -> Result<ResolvedRelation> {
    let ast_unresolved::Relation::Ground {
        mention: ast_unresolved::GroundMention::Receipt { receipt, alias },
        outer,
    } = rel
    else {
        unreachable!("resolve_receipt_read called with a different relation")
    };
    read_compiler_relation(
        crate::defuse::carriers::CompilerRow::scratch(receipt.row()),
        Some(receipt.name().clone()),
        alias,
        access,
        outer,
        fold,
    )
}

/// The read of a compiler-owned row a record or a plan answered with, BY
/// ITS PROOF: the lexical authority stands over the proof, and this road
/// receives no identity. Its producer has already published the heading,
/// so no query-local spelling lookup participates; a name authored on the
/// read makes it an authored access, and the one argumentative operation
/// runs over that access.
fn read_compiler_relation(
    row: crate::defuse::carriers::CompilerRow,
    authored_name: Option<delightql_types::SqlIdentifier>,
    alias: Option<delightql_types::SqlIdentifier>,
    access: ast_unresolved::Access,
    outer: bool,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
) -> Result<ResolvedRelation> {
    let source = ResolvedRelation::over(row, &fold.core.identities)?;
    let source_columns =
        crate::relation::published_ports(&fold.core.identities, &source.semantic_relation())?;
    if source_columns.is_empty() {
        return Err(DelightQLError::validation_error(
            "A plan-scope relation was read before its heading was published",
            "compiler relation identity",
        ));
    }

    // A READ NAMED BY THE AUTHOR: the name makes the read an authored
    // access — the compiler-owned object in the inner FROM and the
    // caller-facing occurrence outside it, exactly the two identities a
    // redirected authored access has — so it takes that road.
    let authored_name = match (authored_name, &alias) {
        (Some(name), _) => Some(name),
        (None, Some(alias)) => Some(alias.clone()),
        (None, None) => None,
    };
    let Some(authored_name) = authored_name else {
        if !matches!(access, ast_unresolved::Access::All) || outer {
            return Err(DelightQLError::validation_error(
                "A direct plan-scope read cannot carry user access metadata",
                "effect plan identity",
            ));
        }
        return Ok(source);
    };

    let access_name = alias.clone().unwrap_or_else(|| authored_name.clone());
    let access_spelling = fold
        .core
        .identities
        .intern(access_name.as_str(), access_name.is_stropped());
    let head = fold.core.identities.authority().plan_read_head(
        GroundForm::Reference(ast_resolved::Relation::InnerRelation {
            pattern: ast_resolved::InnerRelationPattern::Indeterminate {
                identifier: ast_resolved::QualifiedName {
                    namespace_path: NamespacePath::empty(),
                    name: authored_name.clone(),
                },
                subquery: Box::new(source.into_body()),
            },
            alias: Some(access_name.clone()),
            outer,
        }),
        access_spelling,
    )?;
    let access_expr = ast_resolved::Chain::ground(head);

    if matches!(access, ast_unresolved::Access::All) {
        return Ok(ResolvedRelation::answering_for_itself(access_expr));
    }

    // THE ONE ARGUMENTATIVE OPERATION over the read the plan answered
    // with: the slot row consumes the read whole and publishes its own
    // interface under the name the read answers to — the alias, or the
    // formal's own name. A plan carrier is a FORMAL, not an argumentative
    // call: `T(name, id, x)` declares the heading the body addresses as
    // `T.x`, so the name is the carrier's owner.
    ResolvedRelation::patterned(
        super::PatternOperand::Standing(ResolvedRelation::answering_for_itself(access_expr)),
        &access,
        super::PatternOwner::Authored(access_name.clone()),
        fold,
    )?
    .restricted_by_its_own_constraints(&fold.core.identities)
}

/// Resolve a Ground relation variant (named table, view, CTE, or consulted entity).
///
/// This handles passthrough tables, grounded entities, namespace-qualified tables,
/// unqualified tables, CTEs, consulted views/facts, and unknown entities.
pub(super) fn resolve_ground(
    rel: ast_unresolved::Relation,
    access: ast_unresolved::Access,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
) -> Result<ResolvedRelation> {
    use crate::defuse::environment::RelationAnswer;

    let ast_unresolved::Relation::Ground {
        mention:
            ast_unresolved::GroundMention::Named {
                identifier,
                alias,
                mutation_target,
                passthrough,
            },
        outer,
    } = rel
    else {
        unreachable!("resolve_ground called with non-Ground variant");
    };

    // `!!` is evidence about the relation this access reads, and it belongs
    // to the occurrence the access publishes — recorded below, once the
    // relation kind has been settled and before anything is built on it.
    // Every relation built from that occurrence afterwards carries the
    // evidence, so a name, an alias, a CTE binding or a join arm hands it on
    // instead of leaving a later reader to walk the syntax back to a ground
    // name it may no longer have.
    let marked_relation = mutation_target.then(|| {
        fold.core
            .identities
            .intern(identifier.name.as_str(), identifier.name.is_stropped())
    });

    // PASSTHROUGH: skip entity catalog, use schema introspector directly.
    if passthrough {
        let resolved = r_resolve_passthrough(identifier, access, alias, outer, fold)?;
        resolved.noting_mutation_mark(marked_relation, &fold.core.identities)?;
        return Ok(resolved);
    }

    // ONE LOOKUP AUTHORITY, both spellings: the environment owns the
    // closed qualified decision (catalog provider, current definitions,
    // closed miss) exactly as it owns the unqualified ladder.
    let mut serve_bootstrap: Option<ServedBootstrapRead> = None;
    let resolution = if !identifier.namespace_path.is_empty() {
        let (answer, serve) = fold.env.relation_qualified(
            fold.core,
            &identifier.namespace_path,
            &identifier.name,
            fold.config.serve_bootstrap_reads,
        )?;
        if let Some(serve) = serve {
            serve_bootstrap = Some(ServedBootstrapRead {
                canonical: serve.canonical,
                backend_schema: serve.backend_schema,
                namespace_fq: serve.namespace_fq,
            });
        }
        answer
    } else {
        let entity_name = identifier.name.clone();
        fold.env.relation(fold.core, &entity_name, alias.as_ref())?
    };

    let resolved = match resolution {
        RelationAnswer::CTE { entity, frontier } => {
            r_resolve_cte(entity, frontier, identifier, access, alias, outer, fold)
        }
        RelationAnswer::MaterializedRelation(entity_info) => {
            r_resolve_cte(entity_info, None, identifier, access, alias, outer, fold)
        }
        RelationAnswer::DatabaseEntity(entity_info) => {
            r_resolve_database_entity(entity_info, access, alias, outer, fold)
        }
        RelationAnswer::ConsultedView(selected) => {
            r_resolve_consulted_view(selected, access, alias, outer, fold)
        }
        RelationAnswer::DefinedNonRelation { name, entity_type } => {
            Err(defined_non_relation_error(&name, entity_type))
        }
        // THE CATEGORY IS RIGHT AND THE ROAD IS MISSING. Reaching this arm
        // means the executable boundary — which runs before resolution, over
        // the submission's own chains — did not see this occurrence, so the
        // rows were never produced. Refusing here is what keeps a known
        // relation out of the generic-TVF fallback, where its namespace would
        // be stripped and SQL generated against a table that does not exist.
        RelationAnswer::RuntimeServedRelation { name, entity_type } => {
            Err(runtime_served_unreached_error(&name, entity_type))
        }
        RelationAnswer::Ambiguous(message) => Err(DelightQLError::validation_error(
            message,
            "Ambiguous unqualified entity resolution",
        )),
        // A FREE DATA NAME OF A DECLARATION with no bound world: refuse
        // with the grounding teaching — no caller, session, or backend
        // relation answers it ambiently.
        RelationAnswer::DataHole { name, world } => Err(
            crate::defuse::environment::lookup::unbound_data_hole(&name, &world),
        ),
        RelationAnswer::BuiltInFunction => r_resolve_unknown(identifier),
        _ => r_resolve_unknown(identifier),
    }?;
    resolved.noting_mutation_mark(marked_relation, &fold.core.identities)?;
    let resolved = match serve_bootstrap {
        Some(served) => serve_bootstrap_relation(resolved, served, fold)?,
        None => resolved,
    };
    Ok(resolved)
}

/// A bootstrap read a materialization source resolves: served as rows.
struct ServedBootstrapRead {
    canonical: delightql_types::SqlIdentifier,
    backend_schema: Option<String>,
    namespace_fq: String,
}

/// SERVE THE SNAPSHOT the directive already promises: the catalog rows are
/// read HERE, at plan build, on the bootstrap connection — no engine
/// connection reads another's tables — and the resolved read's head becomes
/// a literal table PUBLISHING THE SAME SCOPE, so every downstream binding,
/// pattern restriction and continuation stands unchanged. The compiled
/// source then executes whole on whatever connection attribution selects,
/// in that connection's own dialect.
#[cfg(not(target_arch = "wasm32"))]
fn serve_bootstrap_relation(
    resolved: ResolvedRelation,
    served: ServedBootstrapRead,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
) -> Result<ResolvedRelation> {
    use crate::pipeline::asts::core::{
        AnonRelation, AnonTable, Datum, LiteralValue, TabularBody, TabularRow,
    };
    let Some((scope, outer)) = resolved.ground_head() else {
        return Err(internal_serving_error(
            "a served bootstrap read stands on a ground relation",
        ));
    };

    // The registered column set, in the order the catalog heading was
    // minted from — the same source, so the literal rows align with the
    // scope's own heading. The Arena keeps characters out of reach; the
    // physical names come from the catalog, where they are data.
    let columns: Vec<String> = fold
        .core
        .database
        .schema()
        .get_table_columns(Some(&served.namespace_fq), served.canonical.as_str())?
        .ok_or_else(|| {
            internal_serving_error("a served bootstrap table answers its registered columns")
        })?
        .into_iter()
        .map(|column| column.name.to_string())
        .collect();
    let heading = crate::relation::published_ports(&fold.core.identities, &scope)?;
    if heading.len() != columns.len() {
        return Err(internal_serving_error(
            "a served bootstrap read publishes the registered heading whole",
        ));
    }

    let Some(system) = fold.core.database.system else {
        return Err(internal_serving_error(
            "a served bootstrap read resolves with the system present",
        ));
    };
    let quoted = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
    let from = match &served.backend_schema {
        Some(schema) => format!("{}.{}", quoted(schema), quoted(served.canonical.as_str())),
        None => quoted(served.canonical.as_str()),
    };
    let select = format!(
        "SELECT {} FROM {}",
        columns
            .iter()
            .map(|name| quoted(name))
            .collect::<Vec<_>>()
            .join(", "),
        from
    );

    let connection = system.bootstrap_connection();
    let guard = connection.lock().map_err(|e| {
        DelightQLError::connection_poison_error(
            "Failed to acquire bootstrap lock for a served materialization source",
            format!("Connection was poisoned: {}", e),
        )
    })?;
    let mut statement = guard
        .prepare(&select)
        .map_err(|e| internal_serving_error(&format!("bootstrap-source prepare failed: {e}")))?;
    let width = columns.len();
    let mut literal_rows: Vec<TabularRow<Datum<crate::pipeline::asts::core::Resolved>>> =
        Vec::new();
    let mut rows = statement
        .query([])
        .map_err(|e| internal_serving_error(&format!("bootstrap-source execution failed: {e}")))?;
    while let Some(row) = rows
        .next()
        .map_err(|e| internal_serving_error(&format!("bootstrap-source read failed: {e}")))?
    {
        let mut cells = Vec::with_capacity(width);
        for index in 0..width {
            let value = row.get_ref(index).map_err(|e| {
                internal_serving_error(&format!("bootstrap-source cell read failed: {e}"))
            })?;
            cells.push(Datum::Value(ast_resolved::DomainExpression::Application(
                ast_resolved::FunctionApplication::Ground(served_literal(value)?),
            )));
        }
        literal_rows.push(TabularRow(Box::new(
            crate::pipeline::asts::vocabulary::Vec1::try_from_vec(cells)
                .expect("a catalog table has at least one column"),
        )));
    }
    drop(rows);
    drop(statement);
    drop(guard);

    // ZERO ROWS: the literal geometry is nonempty by type, so an empty
    // snapshot is one all-NULL row behind a false restriction — the same
    // zero-row relation, with its heading intact.
    let empty = literal_rows.is_empty();
    if empty {
        let cells: Vec<_> = (0..width)
            .map(|_| {
                Datum::Value(ast_resolved::DomainExpression::Application(
                    ast_resolved::FunctionApplication::Ground(LiteralValue::Null),
                ))
            })
            .collect();
        literal_rows.push(TabularRow(Box::new(
            crate::pipeline::asts::vocabulary::Vec1::try_from_vec(cells)
                .expect("a catalog table has at least one column"),
        )));
    }

    let served = resolved.payload_restated(
        &fold.core.identities,
        GroundForm::Literal(AnonRelation {
            table: AnonTable {
                body: TabularBody {
                    header: None,
                    rows: crate::pipeline::asts::vocabulary::Vec1::try_from_vec(literal_rows)
                        .expect("the empty snapshot was given its NULL row above"),
                },
            },
            alias: None,
            outer,
        }),
    );
    if !empty {
        return Ok(served);
    }
    {
        let falsehood = ast_resolved::TruthExpression::Comparison(Comparison {
            operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
            left: Box::new(ast_resolved::DomainExpression::Application(
                ast_resolved::FunctionApplication::Ground(LiteralValue::Number("0".to_string())),
            )),
            right: Box::new(ast_resolved::DomainExpression::Application(
                ast_resolved::FunctionApplication::Ground(LiteralValue::Number("1".to_string())),
            )),
        });
        // The head's own read (a leading access) stays the head's; the
        // false restriction stands right behind it.
        let _ = scope;
        Ok(
            served.transparently_behind_head_access(ast_resolved::Transparent::Restrict {
                condition: falsehood,
                origin: crate::pipeline::asts::core::FilterOrigin::Generated,
            }),
        )
    }
}

#[cfg(target_arch = "wasm32")]
fn serve_bootstrap_relation(
    resolved: ResolvedRelation,
    _served: ServedBootstrapRead,
    _fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
) -> Result<ResolvedRelation> {
    Ok(resolved)
}

fn internal_serving_error(message: &str) -> DelightQLError {
    DelightQLError::transformation_error(message, "bootstrap_serving")
}

/// One engine value as the literal it spells. The catalog's declared
/// schemas carry no BLOB columns; meeting one is a teaching, not a panic.
#[cfg(not(target_arch = "wasm32"))]
fn served_literal(
    value: rusqlite::types::ValueRef<'_>,
) -> Result<crate::pipeline::asts::core::LiteralValue> {
    use crate::pipeline::asts::core::LiteralValue;
    use rusqlite::types::ValueRef;
    Ok(match value {
        ValueRef::Null => LiteralValue::Null,
        ValueRef::Integer(value) => LiteralValue::Number(value.to_string()),
        // `{:?}` round-trips an f64: it always writes a decimal point or
        // an exponent, so the literal keeps REAL affinity.
        ValueRef::Real(value) => LiteralValue::Number(format!("{value:?}")),
        ValueRef::Text(bytes) => LiteralValue::String(String::from_utf8_lossy(bytes).into_owned()),
        ValueRef::Blob(_) => {
            return Err(DelightQLError::validation_error_categorized(
                "materialization/bootstrap_blob",
                "a bootstrap BLOB column has no literal spelling to serve",
                "project the column out of the materialization source",
            ))
        }
    })
}

/// Record `!!` on the occurrence a resolved access publishes.
///
/// One place, whatever kind of relation the name turned out to name: a
/// catalog table, a temporary one an earlier step created, a CTE. The mark
/// belongs to this occurrence and not to the definition behind it, so a
/// second, unmarked reference to the same name carries nothing.
pub(super) fn note_mutation_mark(
    relation: Option<crate::names::Spelling>,
    resolved: &ast_resolved::Chain,
    identities: &crate::relation::Planning,
) -> Result<()> {
    if let Some(relation) = relation {
        identities
            .authority()
            .mark_mutation_target(&resolved.semantic_relation(), relation)?;
    }
    Ok(())
}

/// Explain a resolved consulted functor that cannot occupy relation position.
///
/// Kind lookup is centralized before this point, so a defined name never falls
/// through to the absence diagnostic merely because its invocation form is
/// non-relational.
fn defined_non_relation_error(
    name: &SqlIdentifier,
    entity_type: BootstrapEntityType,
) -> DelightQLError {
    let message = match entity_type {
        BootstrapEntityType::DqlDefaultFactFunctionExpression => {
            return DelightQLError::validation_error_categorized(
                crate::uri_registry::subcat::RESOLUTION_FACT_FUNCTION_RELATIONAL_FACE,
                format!(
                    "'{name}' is a default-bearing fact function and has no relational face — \
                     call it as `{name}:(inputs)`, or map that call over a separately supplied \
                     finite relation"
                ),
                "a `_ -> outputs` arm denotes an unbounded input complement",
            );
        }
        BootstrapEntityType::DqlFunctionExpression
        | BootstrapEntityType::DqlHoFunctionExpression
        | BootstrapEntityType::DqlContextAwareFunctionExpression => format!(
            "'{name}' is a function, not a relation — call it as \
             `{name}:(args)`. (A case/scalar function has no relation face \
             `{name}(*)`.)"
        ),
        BootstrapEntityType::DqlHoTemporaryViewExpression => format!(
            "'{name}' is a higher-order view, not a relation — supply its \
             relation argument, for example `{name}(source(*))(*)`"
        ),
        BootstrapEntityType::DqlTemporarySigmaRule | BootstrapEntityType::BinSigmaPredicate => {
            format!(
                "'{name}' is a sigma predicate, not a relation — use it in a \
                 condition rather than accessing `{name}(*)`"
            )
        }
        BootstrapEntityType::BinPseudoPredicate | BootstrapEntityType::DqlEffectRule => format!(
            "'{name}' is a directive, not a relation — invoke the directive \
             rather than accessing `{name}(*)`"
        ),
        BootstrapEntityType::DqlErContextRule => format!(
            "'{name}' is an ER-context rule, not a relation — select it through \
             its declared ER context"
        ),
        other => format!(
            "'{name}' is defined as {}, not a relation",
            other.variant_name()
        ),
    };
    DelightQLError::validation_error(
        message,
        format!("'{name}' resolved to {}", entity_type.variant_name()),
    )
}

/// A runtime-served relation that resolution reached before execution did.
///
/// This is not a statement about the entity's category: it names a relation
/// and publishes a heading, and every position that reaches it through the
/// executable boundary works. What it reports is that this OCCURRENCE was
/// not on a chain the boundary walks — today, a consulted rule's body, whose
/// expansion happens during resolution, after that boundary has run.
fn runtime_served_unreached_error(
    name: &SqlIdentifier,
    entity_type: BootstrapEntityType,
) -> DelightQLError {
    DelightQLError::validation_error(
        format!(
            "'{name}' is a bin relation served by the runtime, and this \
             occurrence escaped the executable boundary that produces its \
             rows — a compiler fence, not a semantic outcome; the direct, \
             bound and consulted spellings all execute"
        ),
        format!("'{name}' resolved to {}", entity_type.variant_name()),
    )
}

/// Handle PASSTHROUGH resolution: skip entity catalog, use schema introspector directly.
/// Best-effort: try to get columns from backend, fall back to opaque glob if not found.
pub(super) fn r_resolve_passthrough(
    identifier: ast_unresolved::QualifiedName,
    access: ast_unresolved::Access,
    alias: Option<SqlIdentifier>,
    outer: bool,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
) -> Result<ResolvedRelation> {
    if identifier.namespace_path.is_empty() {
        return Err(DelightQLError::validation_error(
            "Passthrough table access requires a namespace path (e.g., main/table_name(*))"
                .to_string(),
            "passthrough_requires_namespace".to_string(),
        ));
    }

    // Prefer the mounted catalog, then ask the target introspector for a
    // backend-owned relation that the catalog does not enumerate.
    let (table_schema, canonical_name, passthrough_backend_schema) = match fold
        .core
        .database
        .lookup_passthrough_table_with_namespace(&identifier.namespace_path, &identifier.name)
    {
        Ok(Some((schema, connection_id, canon, passthrough_backend_schema))) => {
            fold.core.track_connection_id(connection_id);
            (Some(schema), Some(canon), passthrough_backend_schema)
        }
        Ok(None) | Err(_) => {
            // Best-effort: table not found in introspector — fall back to opaque
            (None, None, None)
        }
    };

    if let Some(schema) = table_schema {
        bind_physical_relation(
            schema,
            canonical_name.as_ref(),
            passthrough_backend_schema.as_deref(),
            &fold.core.identities,
        )?;
        // Relabel columns with alias if present
        let (aliased, relabeled_cols) =
            relabel_columns_with_alias(schema, &alias, &fold.core.identities)?;

        let _ = relabeled_cols;
        let resolved = super::ResolvedRelation::patterned(
            super::PatternOperand::Read {
                scope: aliased,
                outer: false,
            },
            &access,
            pattern_owner(&alias),
            fold,
        )?
        .restricted_by_its_own_constraints(&fold.core.identities)?;

        // Outerness is the only thing the call site still contributes: the
        // backend lookup that got here IS the passthrough decision, and the
        // spelling it was made from is spent on the scope the pattern
        // resolver published.
        return Ok(resolved.head_marked_outer(outer));
    }

    // Opaque fallback: no column info available. Only an access that names
    // no dimensions can be answered without a heading, and which accesses
    // those are is the access type's own answer.
    if !access.is_whole() {
        return Err(DelightQLError::validation_error(
            format!(
                "Passthrough table '{}/{}' schema not available — only (*) is allowed, not positional binding",
                identifier.namespace_path, identifier.name
            ),
            "passthrough_opaque_glob_only".to_string(),
        ));
    }

    // A passthrough reads a backend table the entity catalog does not
    // describe. It is a relation — it has an identity — and its heading is
    // the target's to publish. The scope travels upward so a reference
    // standing over it learns that nothing was enumerated, rather than being
    // told the name is absent.
    ResolvedRelation::opaque_ground(outer, &fold.core.identities)
}

/// Mark a resolved ground relation as an outer-join operand.
///
/// The head is where a ground relation lives; the continuations that may sit
/// above it (a generated restriction) do not carry outerness.
/// Handle CTE resolution result.
pub(crate) fn r_resolve_cte(
    entity_info: crate::resolution::EntityInfo,
    frontier: Option<crate::defuse::instance::DefinitionFrontier>,
    identifier: ast_unresolved::QualifiedName,
    access: ast_unresolved::Access,
    alias: Option<SqlIdentifier>,
    outer: bool,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
) -> Result<ResolvedRelation> {
    use crate::resolution::EntityDefinition;

    if let Some(frontier) = &frontier {
        crate::defuse::bound_use::judge_recursive_frontier(&fold.config.instances, frontier)?;
    }

    let canonical_name = entity_info.canonical_name.clone();
    let backend_schema = entity_info.backend_schema;
    // Extract the CTE schema
    let EntityDefinition::RelationSchema(cte_schema) = entity_info.definition;
    if canonical_name.is_some() {
        bind_physical_relation(
            cte_schema,
            canonical_name.as_ref(),
            backend_schema.as_deref(),
            &fold.core.identities,
        )?;
    }
    if fold
        .core
        .identities
        .authority()
        .is_plan_scratch(&cte_schema)?
    {
        return Err(DelightQLError::validation_error(
            "Plan scratch must be referenced by scope identity",
            "effect plan identity",
        ));
    }
    // The consult of a USER-DEFINED CTE is an access boundary,
    // same regime as a consulted view: the caller reaches the
    // EXPORTED heading, which answers to the access name (the
    // user's alias, or the CTE name) — bare declarations never
    // leak through, and the export re-roots so a body-internal
    // column spelling cannot reach the SQL (it breaks when the
    // CTE head renames). Compiler-generated CTEs (HO expansion,
    // pipe materialization) are the caller-pattern seam's
    // channel: the seam names their positional columns through
    // the identity stack, so they keep their identity untouched
    // — the boundary law for seam shapes lands with the seam
    // rework, not by breaking it. Argumentative access still
    // declares its own bare lvars either way: the pattern
    // resolver re-declares on selection.
    let source_scope = cte_schema;
    // ONE INSTANCE of the binding: the access boundary is its own relation,
    // and what crosses it, under what name, is the boundary's law.
    let access_name: SqlIdentifier = alias.clone().unwrap_or_else(|| identifier.name.clone());
    let access_spelling = fold
        .core
        .identities
        .intern(access_name.as_str(), access_name.is_stropped());
    let instance =
        fold.core
            .identities
            .authority()
            .derive(crate::relation::RelForm::Instantiate(
                crate::relation::form::InstanceSpec {
                    kind: crate::relation::form::DefinitionKind::Cte,
                    template: source_scope,
                    answers_to: Some(access_spelling),
                },
            ))?;
    let resolved = super::ResolvedRelation::patterned(
        super::PatternOperand::Read {
            scope: instance,
            outer: false,
        },
        &access,
        pattern_owner(&alias),
        fold,
    )?
    .restricted_by_its_own_constraints(&fold.core.identities)?;

    Ok(resolved.head_marked_outer(outer))
}

/// Handle DatabaseEntity resolution result.
pub(super) fn r_resolve_database_entity(
    entity_info: crate::resolution::EntityInfo,
    access: ast_unresolved::Access,
    alias: Option<SqlIdentifier>,
    outer: bool,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
) -> Result<ResolvedRelation> {
    use crate::resolution::EntityDefinition;

    // Extract fields before entity_info is consumed
    let canonical_name = entity_info.canonical_name.clone();
    let entity_backend_schema = entity_info.backend_schema;
    // Extract the table schema
    let EntityDefinition::RelationSchema(table_schema) = entity_info.definition;
    bind_physical_relation(
        table_schema,
        canonical_name.as_ref(),
        entity_backend_schema.as_deref(),
        &fold.core.identities,
    )?;
    // Apply alias if present
    let (aliased, _base_cols) =
        relabel_columns_with_alias(table_schema, &alias, &fold.core.identities)?;

    let resolved = super::ResolvedRelation::patterned(
        super::PatternOperand::Read {
            scope: aliased,
            outer: false,
        },
        &access,
        pattern_owner(&alias),
        fold,
    )?
    .restricted_by_its_own_constraints(&fold.core.identities)?;

    Ok(resolved.head_marked_outer(outer))
}

/// Handle a ConsultedView classification: the opaque token crosses the
/// ONE use entrance, which admits the instance, opens the body, and
/// resolves it in its declaration environment. What comes back here are
/// RESOLVED artifacts — the definition's name, kind, and resolved query
/// — for the access boundary below.
pub(super) fn r_resolve_consulted_view<'db>(
    selected: crate::defuse::bound_use::SelectedRelation<'db>,
    access: ast_unresolved::Access,
    alias: Option<SqlIdentifier>,
    outer: bool,
    fold: &mut super::resolver_fold::ResolverFold<'_, 'db>,
) -> Result<ResolvedRelation> {
    let opened = crate::defuse::bound_use::use_relation(fold, selected)?;
    let (view_name, definition_kind, resolved_query) = opened.resolve_body(fold)?;
    finish_view_access(
        view_name,
        definition_kind,
        resolved_query,
        access,
        alias,
        outer,
        fold,
    )
}

/// The access boundary over an ALREADY-RESOLVED definition body: the
/// boundary is derived FROM the body standing inside the head, so the
/// expansion and the name it is addressed through cannot come apart.
/// Consumes only resolved artifacts — the definition-use act is complete
/// before this runs.
pub(super) fn finish_view_access(
    view_name: SqlIdentifier,
    definition_kind: crate::relation::form::DefinitionKind,
    resolved_query: ast_resolved::Query,
    access: ast_unresolved::Access,
    alias: Option<SqlIdentifier>,
    outer: bool,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
) -> Result<ResolvedRelation> {
    let (effective_alias, answer) =
        access_boundary_answer(&alias, &view_name, &fold.core.identities);
    let effective_name = effective_alias.to_string();
    let head = fold.core.identities.authority().boundary_head(
        GroundForm::Reference(ast_resolved::Relation::ConsultedView {
            body: Box::new(resolved_query),
            outer,
        }),
        crate::relation::builder::Boundary::Instance {
            kind: definition_kind,
            answers_to: Some(answer),
        },
    )?;
    let access_scope = *head.result();
    let base_expr = ast_resolved::Chain::ground(head);

    let _ = access_scope;
    let _ = effective_name;
    // A whole read: the columns answer to the ACCESS name — the user's
    // alias, or the bare entity name of an unaliased access.
    if matches!(access, ast_unresolved::Access::All) {
        return Ok(ResolvedRelation::answering_for_itself(base_expr));
    }
    // THE ONE ARGUMENTATIVE OPERATION over the boundary the view answered
    // with: an unaliased row publishes its binders bare and activates no
    // name, so `p(x), p(y)` stand side by side.
    ResolvedRelation::patterned(
        super::PatternOperand::Standing(ResolvedRelation::answering_for_itself(base_expr)),
        &access,
        pattern_owner(&alias),
        fold,
    )?
    .restricted_by_its_own_constraints(&fold.core.identities)
}

/// Handle Unknown (or unmatched) resolution result.
pub(super) fn r_resolve_unknown(
    identifier: ast_unresolved::QualifiedName,
) -> Result<ResolvedRelation> {
    let (table_name, context) = if !identifier.namespace_path.is_empty() {
        // Construct namespace path string using :: separator (DelightQL
        // format). NOTE: storage order, not iter_reversed — the old
        // context string used iter_reversed and rendered multi-segment
        // paths BACKWARDS (sys::meta as "meta::sys"), invisibly, because
        // the context field is never displayed.
        let ns_str = identifier.namespace_path.fq_string();
        // Report the FULL path the user wrote, never the bare leaf: the
        // leaf-only "Table not found: orders" for `sales.orders(*)`
        // hid the actual mistake (an under-qualified namespace), sent
        // readers hunting for a missing TABLE, and manufactured a
        // false "mount! is broken" diagnosis.
        (
            format!("{}.{}", ns_str, identifier.name),
            format!("Entity '{}' not found in namespace '{}'. The namespace prefix as written did not resolve — check it against your mounts (sys::ns.namespace(*) lists them; a mount under 'data::{}' is reached as 'data::{}.{}'). Other causes: entity not activated, or missing backend schema configuration.", identifier.name, ns_str, ns_str, ns_str, identifier.name)
        )
    } else {
        (
            identifier.name.to_string(),
            "Table or view does not exist in the database".to_string(),
        )
    };

    Err(DelightQLError::TableNotFoundError {
        table_name,
        context,
    })
}

/// Infer a `declared_type` for each anonymous-table column from its literal
/// grid — the `@`-rows ARE the column's declaration. Conservative: a column
/// types only if every cell is a literal of one uniform type (NULLs ignored;
/// INTEGER unifies with REAL as REAL). Any non-literal cell (melt patterns
/// reference outer columns), boolean, or text/numeric mix yields None —
/// that's sqlite-dynamic data with no honest single type. First consumer:
/// corresponding-union NULL pads, whose type comes from the Registry value
/// facts. An untyped pad inside a subquery collapses to text at the pg
/// subquery boundary before the union can resolve it against the typed branch.
fn infer_anon_column_types(
    rows: &crate::pipeline::asts::vocabulary::Vec1<ast_resolved::TabularRow<ast_resolved::Datum>>,
) -> Vec<Option<String>> {
    let num_cols = rows.first().len();
    (0..num_cols)
        .map(|idx| {
            let mut unified: Option<&str> = None;
            for row in rows {
                let Some(ast_resolved::DomainExpression::Application(
                    ast_resolved::FunctionApplication::Ground(value),
                )) = row.0.get(idx).map(ast_resolved::Datum::value)
                else {
                    return None;
                };
                let cell = match &value {
                    ast_resolved::LiteralValue::Null => continue,
                    ast_resolved::LiteralValue::String(_)
                    | ast_resolved::LiteralValue::Symbol(_)
                    | ast_resolved::LiteralValue::Mention(_) => "TEXT",
                    ast_resolved::LiteralValue::Number(n) => {
                        if n.contains(['.', 'e', 'E']) {
                            "REAL"
                        } else {
                            "INTEGER"
                        }
                    }
                    ast_resolved::LiteralValue::Boolean(_) => return None,
                };
                unified = match (unified, cell) {
                    (None, c) => Some(c),
                    (Some(t), c) if t == c => Some(t),
                    (Some("INTEGER"), "REAL") | (Some("REAL"), "INTEGER") => Some("REAL"),
                    _ => return None,
                };
            }
            unified.map(str::to_string)
        })
        .collect()
}

fn infer_anon_column_shapes(
    rows: &crate::pipeline::asts::vocabulary::Vec1<ast_resolved::TabularRow<ast_resolved::Datum>>,
) -> Vec<crate::names::ValueShape> {
    use crate::pipeline::asts::core::Enclyph;
    (0..rows.first().len())
        .map(|idx| {
            let mut shape = None;
            for row in rows {
                let Some(datum) = row.0.get(idx) else {
                    return crate::names::ValueShape::Unknown;
                };
                let current = match datum.value() {
                    ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::Enclyph(Enclyph::Record(_)),
                    ) => crate::names::ValueShape::Record,
                    ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::Enclyph(Enclyph::EmptyRecord(_)),
                    ) => crate::names::ValueShape::Record,
                    ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::Enclyph(Enclyph::Tuple(_)),
                    ) => crate::names::ValueShape::Tuple,
                    _ => return crate::names::ValueShape::Unknown,
                };
                match shape {
                    None => shape = Some(current),
                    Some(existing) if existing == current => {}
                    Some(_) => return crate::names::ValueShape::Unknown,
                }
            }
            shape.unwrap_or_default()
        })
        .collect()
}

/// Loud where knowable: narrowing (`|> .col{...}`) iterates an ARRAY,
/// and when the narrowed column is an anonymous-table column whose
/// every row is a literal OBJECT constructor, the mistake is provable
/// at resolve time — the expansion would walk the object's MEMBERS and
/// return silent all-NULL rows. Refuse naming both remedies. Data-borne
/// values (real columns, mixed or non-literal rows) pass through; their
/// non-array behavior is an open ruling.
pub(super) fn refuse_knowable_object_narrowing(
    column: &str,
    source: &ast_resolved::Chain,
    identities: &crate::relation::Planning,
) -> Result<()> {
    let scope = source.semantic_relation();
    let sought = identities.canonical(identities.intern(column, false));
    let heading = crate::relation::published_ports(identities, &scope)?;
    let Some(idx) = heading
        .iter()
        .position(|candidate| identities.published_sym(candidate.column()) == Some(sought))
    else {
        return Ok(());
    };
    let occurrence = heading
        .iter()
        .nth(idx)
        .copied()
        .expect("the named position came from this exhaustive heading");
    if identities.facts(occurrence.column()).shape == crate::names::ValueShape::Record {
        return Err(DelightQLError::validation_error_categorized(
            "narrowing/object_literal",
            format!(
                "narrowing iterates an array — every row of '{column}' is a single \
                 object. Path into the object instead: ({column}:{{.field}}), or \
                 spell the one-element sequence: [{{...}}]."
            ),
            "brace narrowing",
        ));
    }
    Ok(())
}

/// Resolve an Anonymous relation variant (inline table with rows/headers).
///
/// Handles header resolution, row value resolution, and QUA schema conformance.
/// Resolve an expression written inside an anonymous table against the row
/// that ENCLOSES it.
///
/// A header and a data cell both reach out of the anonymous relation for the
/// names they use — the anonymous relation has no heading of its own until
/// these are resolved. The context swap is one act, so a header and a cell
/// cannot come to disagree about which columns were in scope.
fn resolve_against_outer_context(
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
    expression: ast_unresolved::DomainExpression,
) -> Result<ast_resolved::DomainExpression> {
    // The literal has no heading of its own yet, so its headers and cells
    // are read over the row in view, FLAT: nothing here shadows anything.
    let saved_in_correlation = fold.in_correlation;
    fold.in_correlation = false;
    let result = fold.resolve_flat_over_the_row(expression);
    fold.in_correlation = saved_in_correlation;
    result
}

pub(super) fn resolve_anonymous(
    anon: ast_unresolved::AnonRelation,
    fold: &mut super::resolver_fold::ResolverFold,
) -> Result<ResolvedRelation> {
    let ast_unresolved::AnonRelation {
        table:
            ast_unresolved::AnonTable {
                body: ast_unresolved::TabularBody { header, rows },
            },
        alias: relation_alias,
        outer,
    } = anon;
    // `_` is the disregarded slot: no term of its own, no name, and the
    // classification below reads that absence directly.
    let column_headers = header
        .as_ref()
        .map(|row| {
            row.iter()
                .map(|item| match &item.slot {
                    crate::pipeline::asts::core::Slot::Anon => Ok(None),
                    _ => item.term().map(Some).ok_or_else(|| {
                        DelightQLError::parse_error("a tabular header slot has a domain term")
                    }),
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;

    let scope_answer = relation_alias.as_ref().map(|alias| {
        fold.core
            .identities
            .intern(alias.as_str(), alias.is_stropped())
    });
    // Convert rows from unresolved to resolved format
    // Resolve anonymous table data rows with outer_context for melt/unpivot
    let resolved_rows = rows.clone().try_map(|row| {
        let resolved_values = (*row.0).try_map(|datum| {
            let sparse_column = match &datum {
                ast_unresolved::Datum::SparseFill { column, .. } => Some(column.clone()),
                ast_unresolved::Datum::Value(_) => None,
            };
            let val = datum.into_value();
            match val {
                ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(value),
                ) => {
                    let value = ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::Ground(value),
                    );
                    Ok(match sparse_column {
                        Some(column) => ast_resolved::Datum::SparseFill {
                            column,
                            fallback: match value {
                                ast_resolved::DomainExpression::Application(
                                    ast_resolved::FunctionApplication::Ground(ref value),
                                ) => value.clone(),
                                _ => unreachable!(),
                            },
                        },
                        None => ast_resolved::Datum::Value(value),
                    })
                }
                // Resolve column references and other expressions.
                // This enables melt/unpivot patterns like:
                // _(attr, val @ "name", first_name; "id", user_id)
                //                       ^^^^^^^^^^      ^^^^^^^
                _ => resolve_against_outer_context(fold, val).map(ast_resolved::Datum::Value),
            }
        })?;
        Ok::<_, DelightQLError>(crate::pipeline::asts::core::TabularRow(Box::new(
            resolved_values,
        )))
    })?;

    // An lvar cannot appear both in a header and in the data rows of
    // the same anonymous table: the header is the probe,
    // a row lvar is a candidate — the same name in both makes the
    // membership vacuously true, and in the relational forms it
    // collides the declaration with the reference.
    if let Some(headers) = &column_headers {
        for header in headers {
            let Some(ast_unresolved::DomainExpression::Reference(Reference::Named(
                NamedReference(AuthoredColumn { name, .. }),
            ))) = header
            else {
                continue;
            };
            let repeated = rows.iter().any(|row| {
                row.iter().any(|datum| {
                    let cell = datum.value();
                    matches!(cell,
                        ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn { name: cell_name, .. })))
                            if delightql_types::SqlIdentifier::str_eq(cell_name.as_str(), name))
                })
            });
            if repeated {
                return Err(crate::error::DelightQLError::validation_error_categorized(
                    "resolution/anon/header_row_lvar",
                    format!(
                        "lvar '{}' appears both as a header and in the data rows of the same anonymous table",
                        name
                    ),
                    "the header is the probe and a row lvar is a candidate — probing a column against itself is vacuously true; drop the self-candidate or rename the header",
                ));
            }
        }
    }

    // Literal-grid type inference: the rows are the columns' declaration.
    let inferred_types = infer_anon_column_types(&resolved_rows);
    let inferred_shapes = infer_anon_column_shapes(&resolved_rows);

    // Classify the complete heading before the relation exists. Each member
    // chooses one closed anonymous-slot law; none chooses an owner,
    // addressing disposition, or destination scope.
    // THE HEADER READS BY THE SLOT VOCABULARY — bind, reuse, ground,
    // disregard — the same row the caller pattern reads. Only binders
    // publish: a repeated binder is the same variable twice (one published
    // column and an equality between the positions), `_` disregards, and a
    // ground or computed term constrains and publishes nothing.
    enum HeaderRole {
        Binder,
        /// The same variable again: an equality with the position that
        /// bound it, and no publication of its own.
        Repeat {
            first: usize,
        },
        /// `_` — consumed, constrained by nothing, published as nothing.
        Disregard,
        Constrains,
    }
    let mut roles: Vec<HeaderRole> = Vec::new();
    let (header_values, slots) = if let Some(headers) = &column_headers {
        let mut seen: Vec<(String, usize)> = Vec::new();
        let mut values = Vec::with_capacity(headers.len());
        let mut slots = Vec::with_capacity(headers.len());
        for (idx, header) in headers.iter().enumerate() {
            let declared_type = inferred_types.get(idx).cloned().flatten();
            let shape = inferred_shapes.get(idx).copied().unwrap_or_default();
            match header {
                Some(ast_unresolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(AuthoredColumn { name, .. }),
                ))) => {
                    // A BINDER IS A VARIABLE, and only variables unify: a
                    // stropped spelling is an authored NAME, so repeating
                    // one is a name collision (minted apart), not a
                    // self-unification.
                    if let Some(&(_, first)) = (!name.is_stropped())
                        .then(|| {
                            seen.iter().find(|(spelt, _)| {
                                delightql_types::SqlIdentifier::str_eq(spelt, name.as_str())
                            })
                        })
                        .flatten()
                    {
                        roles.push(HeaderRole::Repeat { first });
                        slots.push(crate::relation::form::AnonymousSlot::Constraint {
                            position: idx as u32,
                            declared_type,
                            shape,
                        });
                        // The value is written after the derivation, when the
                        // first binder's port exists.
                        values.push(None);
                        continue;
                    }
                    if !name.is_stropped() {
                        seen.push((name.to_string(), idx));
                    }
                    let named = fold
                        .core
                        .identities
                        .intern(name.as_str(), name.is_stropped());
                    roles.push(HeaderRole::Binder);
                    slots.push(crate::relation::form::AnonymousSlot::Binder {
                        position: idx as u32,
                        named,
                        declared_type,
                        shape,
                    });
                    values.push(None);
                }
                None => {
                    // `_` — the disregarded slot.
                    roles.push(HeaderRole::Disregard);
                    slots.push(crate::relation::form::AnonymousSlot::Constraint {
                        position: idx as u32,
                        declared_type,
                        shape,
                    });
                    values.push(None);
                }
                Some(_) => {
                    // A computed header names a column of the ENCLOSING row —
                    // `_(upper:(description) @ …)` probes the outer relation's
                    // `description`. It resolves against the same context the
                    // data rows do, because it is a reference out of the same
                    // place.
                    let header = header.clone().expect("the Some arm holds a term");
                    roles.push(HeaderRole::Constrains);
                    let resolved_expr = resolve_against_outer_context(fold, header.clone())?;
                    let slot = match &resolved_expr {
                        ast_resolved::DomainExpression::Application(
                            ast_resolved::FunctionApplication::Ground(_),
                        ) => crate::relation::form::AnonymousSlot::Literal {
                            position: idx as u32,
                            declared_type,
                            shape,
                        },
                        ast_resolved::DomainExpression::Application(_) => {
                            crate::relation::form::AnonymousSlot::Constraint {
                                position: idx as u32,
                                declared_type,
                                shape,
                            }
                        }
                        other => panic!("catch-all hit in relation_resolver.rs resolve_inline_relation (DomainExpression column name): {:?}", other),
                    };
                    slots.push(slot);
                    values.push(Some(resolved_expr));
                }
            }
        }
        (Some(values), slots)
    } else {
        let num_cols = resolved_rows.first().len();
        let slots = (0..num_cols)
            .map(|idx| crate::relation::form::AnonymousSlot::Inferred {
                position: idx as u32,
                declared_type: inferred_types.get(idx).cloned().flatten(),
                shape: inferred_shapes.get(idx).copied().unwrap_or_default(),
            })
            .collect();
        (None, slots)
    };

    let resolved_schema =
        fold.core
            .identities
            .authority()
            .derive(crate::relation::RelForm::Anonymous(
                crate::relation::form::AnonymousSpec {
                    shape: crate::relation::form::AnonymousShape::Tabular,
                    slots: &slots,
                    answers_to: scope_answer,
                },
            ))?;
    let ports = crate::relation::published_ports(&fold.core.identities, &resolved_schema)?;
    let self_reference = |port: crate::relation::PortId| {
        ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence::engine(port),
        )))
    };
    let resolved_header = header_values.map(|values| {
        let sparse = header
            .as_ref()
            .expect("resolved headers preserve an authored header");
        crate::pipeline::asts::core::TabularRow(Box::new(
            crate::pipeline::asts::vocabulary::Vec1::try_from_vec(
                values
                    .into_iter()
                    .zip(&ports)
                    .zip(&roles)
                    .zip(sparse.iter())
                    .map(|(((value, port), role), authored)| {
                        let slot = match role {
                            // The repeated binder REUSES the position that
                            // bound the variable: the stored term is that
                            // position's occurrence, and the constraint the
                            // analyzer writes from it is the equality the
                            // repetition means.
                            HeaderRole::Repeat { first } => {
                                ast_resolved::Slot::classify(self_reference(ports[*first]))
                            }
                            HeaderRole::Disregard => ast_resolved::Slot::Anon,
                            HeaderRole::Binder | HeaderRole::Constrains => {
                                ast_resolved::Slot::classify(
                                    value.unwrap_or_else(|| self_reference(*port)),
                                )
                            }
                        };
                        ast_resolved::HeaderItem {
                            slot,
                            sparse: authored.sparse,
                        }
                    })
                    .collect(),
            )
            .expect("a tabular header is nonempty"),
        ))
    });
    let resolved_relation = ast_resolved::AnonRelation {
        table: ast_resolved::AnonTable {
            body: ast_resolved::TabularBody {
                header: resolved_header,
                rows: resolved_rows,
            },
        },
        alias: relation_alias,
        outer,
    };

    let chain = ast_resolved::Chain::ground(fold.core.identities.authority().reading(
        crate::relation::builder::ReadHead::Anonymous {
            relation: resolved_relation,
            published: resolved_schema,
        },
    )?);

    // ONLY BINDERS PUBLISH. A slot that repeats, disregards, or constrains
    // is consumed: its position stays physical (the grid still emits the
    // cell, and the constraint the analyzer writes still reads it), and the
    // read's own access narrows the heading to the binders.
    let published: Vec<bool> = roles
        .iter()
        .map(|role| matches!(role, HeaderRole::Binder))
        .collect();
    if !roles.is_empty() && published.iter().any(|publishes| !publishes) {
        if published.iter().all(|publishes| !publishes) {
            // ZERO-WIDTH IS LAWFUL (RULINGS 2026-08-19): a slot row whose
            // every position is consumed denotes the relation with zero
            // columns, keeping its row count. The grid still emits every
            // physical cell and the analyzer's constraints still read
            // them — the positions ride as dependencies, the published
            // heading is empty.
            let chain = fold.core.identities.authority().extend(
                chain,
                crate::relation::builder::StepOp::Access {
                    shape: crate::relation::form::AccessShape::Empty,
                    slots: &[],
                    dependencies: &ports,
                },
            )?;
            return Ok(ResolvedRelation::answering_for_itself(chain));
        }
        let positions: Vec<_> = ports
            .iter()
            .zip(&published)
            .filter(|(_, publishes)| **publishes)
            .map(|(port, _)| crate::relation::pending::Position::Authored {
                expr: ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                    ColumnOccurrence::engine(*port),
                ))),
                naming: None,
            })
            .collect();
        let (narrowed, _) = fold.core.identities.authority().bind(
            crate::relation::pending::Pending::Publication {
                input: resolved_schema,
                publishes: crate::relation::pending::Publishes::Anew,
                // A compiler narrowing to the binders — the read's own
                // access, not a pipe stage: the positions keep the
                // publication they proposed.
                why: crate::relation::form::ProjectWhy::Restate,
                positions,
            },
        )?;
        let chain = fold.core.identities.authority().reland(chain, narrowed)?;
        return Ok(ResolvedRelation::answering_for_itself(chain));
    }

    let _ = resolved_schema;
    Ok(ResolvedRelation::answering_for_itself(chain))
}

/// Handles higher-order view expansion and ordinary relational calls through
/// the shared call carrier.
///
/// What comes back is the call's SEALED OUTCOME, not a chain beside a
/// scope beside a name: the relation, what answers over it, the answer
/// this read was given, and whether the normalizer landed a piped
/// relation in the call's one open parameter are written here, in one
/// act, and only [`super::pipe_form::CallOutcome::crossed_if_landed`]
/// opens them. A LANDED CALL IS A PIPE FORM (fundamentals: a PIPE FORM is
/// a PIPE OPERATOR, a CALL, or a REDUCTION), so the outcome — not a
/// caller — decides whether the barrier stands.
pub(super) fn resolve_functor_call(
    call: ast_unresolved::FunctorCall,
    alias: Option<delightql_types::SqlIdentifier>,
    access: ast_unresolved::Access,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
    caller_row: &mut super::CallerRow,
) -> Result<super::pipe_form::CallOutcome> {
    // The authored call and the answer written at its position go into the
    // sealing WHOLE. The landing is read off that same call's own
    // arguments, the answer is the one the resolution is handed, and what
    // comes back is what that one resolution answered — so the four facts
    // the outcome holds have one origin between them.
    super::pipe_form::CallOutcome::of(call, alias, |call, alias| {
        resolve_functor_call_inner(call, alias, access, fold, caller_row)
    })
}

#[allow(clippy::too_many_arguments)]
fn resolve_functor_call_inner(
    call: ast_unresolved::FunctorCall,
    // The name the READ answers to, from the relation occurrence the call
    // stands in. Call identity carries none.
    alias: Option<delightql_types::SqlIdentifier>,
    access: ast_unresolved::Access,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
    caller_row: &mut super::CallerRow,
) -> Result<ResolvedRelation> {
    let reference = call.callee;
    let ast_unresolved::FunctorCall {
        marks, arguments, ..
    } = call;
    let function = reference.name_text().to_string();
    let function_stropped = reference.name_identifier().is_stropped();
    let namespace = (!reference.namespace_texts().is_empty()).then(|| reference.namespace_texts());

    // THE PIPE AND ITS LANDING ARE ONE MEMBER, read off the call itself.
    // Nothing is taken apart and nothing is rebuilt: the row that reaches
    // the higher-order road is the row the build made, landed member and
    // all, so there is no filtered copy to keep in step with an index and
    // no index for a copy to disagree with. What that road owes is the
    // SHAPE the landing needs — a relation formal, and a complete left
    // prefix beside it.
    let piped = arguments
        .judged()?
        .landed()
        .map(|landed| landed.relation.clone());

    // Higher-order view invocation: the ONE definition-use entrance for
    // parameterized definitions. Naming is judged here (an authored
    // qualifier or the enlisted candidate set); everything else —
    // selection, the landing's shape, the semantic actual key, admission,
    // opening, squishing, environment — is the authority's. A miss falls
    // through to the table and TVF roads below. (The pre-grounded arm
    // that once iterated `grounding` here was dead: this road always
    // starts with `grounding = None`.)
    {
        let naming = match &namespace {
            Some(ns) => crate::defuse::ho::HoNaming::Qualified(ns),
            None => crate::defuse::ho::HoNaming::Enlisted,
        };
        if let Some(resolved) = crate::defuse::ho::use_ho_invocation(
            naming,
            &function,
            function_stropped,
            &access,
            &arguments,
            piped,
            caller_row,
            fold,
            alias.clone(),
        )? {
            return Ok(resolved);
        }
    }

    // A glob-only relation access is the table spelling, not a zero-argument
    // TVF.  Keep this decision after the higher-order lookup roads so names
    // such as `exists(*)` still expand as views, while CTE labels and ordinary
    // tables retain the established `name(*)` default.
    let table_default = arguments.is_empty()
        || matches!(
            arguments.scalar_members(),
            [
                crate::pipeline::asts::core::operators::ScalarArgument::Spread(
                    crate::pipeline::asts::core::Spread::Glob(_)
                )
            ]
        );
    if table_default && !function.ends_with('!') {
        let identifier = ast_unresolved::QualifiedName {
            namespace_path: ast_unresolved::NamespacePath::from_parts(
                namespace.unwrap_or_default(),
            )
            .map_err(|error| {
                DelightQLError::parse_error(format!(
                    "invalid namespace on relation '{}': {:?}",
                    function, error
                ))
            })?,
            name: function.clone().into(),
        };
        return resolve_ground(
            ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Named {
                    identifier,
                    alias,
                    mutation_target: false,
                    passthrough: false,
                },
                outer: false,
            },
            access,
            fold,
        );
    }

    // A TVF argument that names a column of the enclosing row is resolved
    // HERE, where that row's columns are in hand: `json_each(|1|)` and
    // `json_each(data)` both have to reach an occurrence before generation,
    // and SQL has no ordinal syntax to fall back on.
    //
    // What comes back is a RESOLVED expression, kept beside the authored
    // list. Writing an occurrence back into the authored argument would put
    // a resolved state in a tree nobody has resolved.
    let member_domains: Vec<Option<&ast_unresolved::DomainExpression>> = match &arguments {
        crate::pipeline::asts::core::operators::CallArguments::None => Vec::new(),
        crate::pipeline::asts::core::operators::CallArguments::HigherOrder(part) => part
            .members()
            .iter()
            .map(|argument| argument.scalar_domain())
            .collect(),
        crate::pipeline::asts::core::operators::CallArguments::Scalar(members) => members
            .iter()
            .map(|member| member.scalar_domain())
            .collect(),
    };
    let mut bound_arguments: Vec<Option<ast_resolved::DomainExpression>> =
        vec![None; member_domains.len()];
    // A CALL'S AUTHORED ARGUMENTS ARE READ OVER THE ROW IN VIEW, flat:
    // the row the call stands in, and whatever encloses it.
    if fold.lexical.encloses_a_row() {
        for (index, domain) in member_domains.iter().enumerate() {
            if let Some(ast_unresolved::DomainExpression::Reference(Reference::Ordinal(
                ref ordinal,
            ))) = domain
            {
                // THE FRONTIER ANSWERS THE ORDINAL, over everything in view
                // at the call: the position it names, or the refusal it
                // earned.
                use super::unification::{ColumnReference, UnificationResult};
                let reference = ColumnReference::Ordinal {
                    position: ordinal.position,
                    reverse: ordinal.reverse,
                    qualifier: ordinal.qualifier.clone(),
                };
                let mut witness = super::Witness::default();
                let resolved = fold.lexical.flatly(|position| {
                    position.address(reference, false, &mut witness, &fold.core.identities)
                })?;
                let occurrence = match resolved {
                    UnificationResult::Resolved(occurrence) => occurrence,
                    UnificationResult::Unresolved(column) => {
                        return Err(DelightQLError::column_not_found_error(
                            column,
                            "in TVF argument",
                        ))
                    }
                    UnificationResult::Ambiguous { column, tables } => {
                        return Err(DelightQLError::validation_error_categorized(
                            "resolution/ambiguous",
                            format!(
                                "Column '{column}' in TVF argument is ambiguous. Could refer to: {}",
                                tables.join(", ")
                            ),
                            "TVF argument",
                        ))
                    }
                    UnificationResult::Opaque => {
                        return Err(crate::pipeline::resolver::opaque_reference_refusal())
                    }
                    UnificationResult::Refused(refusal) => return Err(refusal.into_error()),
                };
                bound_arguments[index] = Some(ast_resolved::DomainExpression::Reference(
                    Reference::Named(NamedReference(occurrence)),
                ));
            } else if let Some(ast_unresolved::DomainExpression::Reference(Reference::Named(
                NamedReference(AuthoredColumn {
                    name, qualifier, ..
                }),
            ))) = domain
            {
                // A named argument is a reference and resolves as one. An
                // ordinal beside it already does, and leaving the name alone
                // carries an authored lvar past the phase that ends them —
                // the lowering then has a spelling where it needs a column.
                use super::unification::{ColumnReference, UnificationResult};
                let reference = ColumnReference::Named {
                    name: name.clone(),
                    qualifier: qualifier.clone(),
                };
                let mut witness = super::Witness::default();
                let resolved = fold.lexical.flatly(|position| {
                    position.address(reference, false, &mut witness, &fold.core.identities)
                })?;
                match resolved {
                    UnificationResult::Resolved(occurrence) => {
                        bound_arguments[index] = Some(ast_resolved::DomainExpression::Reference(
                            Reference::Named(NamedReference(occurrence)),
                        ));
                    }
                    UnificationResult::Opaque => {
                        return Err(crate::pipeline::resolver::opaque_reference_refusal())
                    }
                    UnificationResult::Unresolved(column) => {
                        return Err(DelightQLError::column_not_found_error(
                            column,
                            "in TVF argument",
                        ))
                    }
                    UnificationResult::Refused(refusal) => return Err(refusal.into_error()),
                    UnificationResult::Ambiguous { column, tables } => {
                        return Err(DelightQLError::validation_error_categorized(
                            "resolution/ambiguous",
                            format!(
                                "Ambiguous column '{}' exists in scopes: {}",
                                column,
                                tables.join(", ")
                            ),
                            "in TVF argument",
                        ))
                    }
                }
            }
        }
    }

    // A bin relation the catalog knows must not fall through to the TVF
    // road: the fallback strips its namespace and compiles a phantom table
    // reference under the bare name. It is refused HERE with its identity —
    // execution for this entity lives at the effect boundary, which only
    // reaches the submission's own chain and its bindings.
    if let Some(ref ns) = namespace {
        let fq = ns.join("::");
        crate::defuse::bound_use::refuse_served_bin_relation(
            fold.core,
            fold.env,
            &function,
            function_stropped,
            &fq,
            runtime_served_unreached_error,
        )?;
    }

    // A TVF the catalog describes publishes a known heading; one it does
    // not is the default-transpilation case, and its heading is the
    // target's until a caller pattern declares one.
    let described = get_tvf_schema(&function, alias.as_deref(), &fold.core.identities);

    if described.is_none() {
        if fold.config.permissive {
            eprintln!(
                "WARNING: Unknown TVF '{}' - treating as generic table function",
                function
            );
            // Keep Unknown schema
        } else {
            return Err(DelightQLError::validation_error_categorized(
                crate::uri_registry::subcat::RESOLUTION_CALLABLE_UNKNOWN,
                format!("Unknown TVF: {function}"),
                "unknown table-valued callable",
            ));
        }
    }

    // A TVF heading — the ampersand form's tail or a second parens — is
    // heading-shaped: each slot NAMES a published column of the function's
    // schema, an ordered projection, never a slot-by-slot binding (the
    // function's arity lives in its argument list, not its heading).
    // Binding happens here because resolution is where authored characters
    // stop: the refiner reads occurrences and refuses an authored lvar.
    let (resolved_spec, schema) = match (&access, described) {
        (ast_unresolved::Access::Slots(slots), Some(scope)) => {
            let source_heading = crate::relation::published_ports(&fold.core.identities, &scope)?;
            let (table_name, stropped) = match &alias {
                Some(alias) => (alias.as_str(), alias.is_stropped()),
                None => (function.as_str(), false),
            };
            let hint = fold.core.identities.intern(table_name, stropped);
            let mut selected = Vec::with_capacity(slots.len());
            let mut bound: Vec<crate::names::Sym> = Vec::new();
            for slot in slots {
                let ast_unresolved::Slot::Bind(crate::pipeline::asts::core::WrittenBinder {
                    name,
                    ..
                }) = slot
                else {
                    return Err(DelightQLError::validation_error(
                        format!(
                            "the heading of TVF '{}' is an ordered projection of \
                             the function's columns — each slot must be a bare \
                             column name",
                            table_name
                        ),
                        "in TVF heading",
                    ));
                };
                let sym = fold
                    .core
                    .identities
                    .canonical(fold.core.identities.intern(name, name.is_stropped()));
                // A heading's names are programmer-authored, so they obey the
                // uniqueness a projection's do. Without this the second slot
                // is published as `name_2` and then READ as though the
                // function offered a column by that name.
                if bound.contains(&sym) {
                    return Err(DelightQLError::validation_error_categorized(
                        "constraint",
                        format!(
                            "Duplicate column '{name}' in the heading of TVF \
                             '{table_name}': programmer-authored names must be \
                             unique. Rename one with 'as' to disambiguate"
                        ),
                        "in TVF heading",
                    ));
                }
                bound.push(sym);
                // Every carrier is enumerated, never the first: the hard-coded
                // schemas happen to publish unique names, but the contract
                // here is a published schema and runtime introspection does
                // not establish that.
                let mut carriers = source_heading.iter().copied().filter(|column| {
                    fold.core.identities.published_sym(column.column()) == Some(sym)
                });
                let source = match (carriers.next(), carriers.next()) {
                    (Some(source), None) => source,
                    (None, _) => {
                        return Err(DelightQLError::column_not_found_error(
                            name.to_string(),
                            format!("in the heading of TVF '{}'", table_name),
                        ))
                    }
                    (Some(_), Some(_)) => {
                        return Err(DelightQLError::validation_error_categorized(
                            "resolution/ambiguous",
                            format!(
                                "TVF '{table_name}' publishes '{name}' more than \
                                 once, so a heading slot naming it reaches no \
                                 single column"
                            ),
                            "in TVF heading",
                        ))
                    }
                };
                selected.push(crate::relation::form::ProjectSlot::Carried {
                    source,
                    naming: crate::relation::form::Naming::Inherited,
                });
            }
            let selected_scope =
                fold.core
                    .identities
                    .authority()
                    .derive(crate::relation::RelForm::Access(
                        crate::relation::form::AccessSpec {
                            input: scope,
                            shape: crate::relation::form::AccessShape::Named,
                            slots: &selected,
                            dependencies: &[],
                        },
                    ))?;
            let output_scope =
                fold.core
                    .identities
                    .authority()
                    .derive(crate::relation::RelForm::Export(
                        crate::relation::form::ExportSpec {
                            input: selected_scope,
                            why: crate::relation::form::ExportWhy::Alias { answer: hint },
                        },
                    ))?;
            let occurrences =
                crate::relation::published_ports(&fold.core.identities, &output_scope)?
                    .into_iter()
                    .map(ast_resolved::Slot::Bind)
                    .collect();
            let occurrences = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(occurrences)
                .expect("a TVF heading with slots binds at least one");
            (ast_resolved::Access::Slots(occurrences), output_scope)
        }
        // The target published nothing to project FROM, so the caller
        // pattern is not a projection of a heading — it IS the heading. One
        // slot per dimension of the full width, declared at the mention.
        // Nothing checks it against the target, for the same reason nothing
        // checks `upper:(x)`: a name this compiler cannot verify is the
        // target's to disagree with.
        (ast_unresolved::Access::Slots(slots), None) => {
            let (table_name, stropped) = match &alias {
                Some(alias) => (alias.as_str(), alias.is_stropped()),
                None => (function.as_str(), false),
            };
            let hint = fold.core.identities.intern(table_name, stropped);
            let mut bound: Vec<crate::names::Sym> = Vec::new();
            let declared_slots: Vec<_> = slots
                .iter()
                .enumerate()
                .map(|(position, slot)| {
                    // A named slot publishes; a slot that names nothing is a
                    // dimension all the same, and holds its place latently.
                    let declared = match slot {
                        ast_unresolved::Slot::Bind(
                            crate::pipeline::asts::core::WrittenBinder { name, .. },
                        ) => Some((
                            name.as_str(),
                            fold.core.identities.intern(name, name.is_stropped()),
                        )),
                        _ => None,
                    };
                    let published = declared.map(|(_, spelling)| spelling);
                    if let Some((name, spelling)) = declared {
                        let sym = fold.core.identities.canonical(spelling);
                        if bound.contains(&sym) {
                            return Err(DelightQLError::validation_error_categorized(
                                "constraint",
                                format!(
                                    "Duplicate column '{}' in the declared heading of \
                                 '{table_name}': programmer-authored names must be \
                                 unique. Rename one with 'as' to disambiguate",
                                    name
                                ),
                                "in TVF heading",
                            ));
                        }
                        bound.push(sym);
                    }
                    Ok(crate::relation::form::AnonymousSlot::Declared {
                        position: position as u32,
                        named: published,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let declared_scope =
                fold.core
                    .identities
                    .authority()
                    .derive(crate::relation::RelForm::Anonymous(
                        crate::relation::form::AnonymousSpec {
                            shape: crate::relation::form::AnonymousShape::Tabular,
                            slots: &declared_slots,
                            answers_to: Some(hint),
                        },
                    ))?;
            let ports = crate::relation::published_ports(&fold.core.identities, &declared_scope)?;
            let mut occurrences = Vec::with_capacity(ports.len());
            for port in ports {
                // A heading slot NAMES a dimension, which is what a binding
                // slot is. It stays one across the phase edge: only the
                // payload changes, from the written name to the column.
                occurrences.push(ast_resolved::Slot::Bind(port));
            }
            let occurrences = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(occurrences)
                .expect("a declared heading has at least one dimension");
            (ast_resolved::Access::Slots(occurrences), declared_scope)
        }
        // Nothing was declared and nothing is published: the relation still
        // has an identity, and only its heading is unknown.
        (_, None) => {
            let opaque_scope = fold
                .core
                .identities
                .authority()
                .derive(crate::relation::RelForm::Opaque)?;
            (resolve_schema_free_access(&access)?, opaque_scope)
        }
        (_, Some(scope)) => (resolve_schema_free_access(&access)?, scope),
    };

    // Resolve namespace to physical backend schema + connection routing.
    // Same logic as Ground passthrough: resolve namespace, track connection_id,
    // replace DQL namespace path with physical schema name for SQL generation.
    let namespace_path = namespace.as_ref().map(|parts| {
        NamespacePath::from_parts(parts.clone()).expect("canonical reference namespace is nonempty")
    });
    let resolved_namespace = if let Some(ref ns) = namespace_path {
        if !ns.is_empty() {
            match fold.core.database.resolve_namespace(ns) {
                Ok(Some((physical_schema, conn_id))) => {
                    fold.core.track_connection_id(conn_id);
                    // physical_schema=None means tables are in `main` of that connection
                    physical_schema.map(|s| NamespacePath::single(&*s))
                }
                _ => namespace_path.clone(),
            }
        } else {
            namespace_path.clone()
        }
    } else {
        None
    };

    // Convert ho_arguments from Unresolved to Resolved phase for non-HO TVFs.
    // TARGET FALLBACK PRESERVES THE COMPLETE ARGUMENT ROW: an argument the
    // binder already resolved travels as itself; one that neither bound nor
    // converts — a relation or callable actual the target row cannot carry —
    // REFUSES rather than silently vanishing from the emitted SQL.
    let mut resolved_ho_arguments: Vec<
        crate::pipeline::asts::core::operators::HoArgument<crate::pipeline::asts::core::Resolved>,
    > = Vec::with_capacity(member_domains.len());
    for (domain, bound) in member_domains.iter().zip(bound_arguments) {
        let value = match (bound, domain) {
            (Some(value), _) => value,
            (None, Some(domain)) => convert_domain_expression(domain, &fold.core.identities)?,
            (None, None) => {
                return Err(DelightQLError::validation_error_categorized(
                    crate::uri_registry::subcat::RESOLUTION_CALLABLE_UNKNOWN,
                    format!(
                        "no DQL callable '{function}' exists, and the default target \
                         transpilation cannot carry its relation or callable \
                         argument — a target function row holds values only. \
                         Every authored argument must survive the fallback, so the \
                         call refuses instead of dropping one."
                    ),
                    "unknown callable with a relation argument",
                ));
            }
        };
        resolved_ho_arguments.push(crate::pipeline::asts::core::operators::HoArgument::Value(
            crate::pipeline::asts::core::ArgumentValue::plain(value),
        ));
    }

    let function_spelling = fold.core.identities.intern(function.as_str(), false);
    let function_namespace = resolved_namespace
        .as_ref()
        .map(|path| {
            path.iter()
                .map(|item| {
                    fold.core
                        .identities
                        .intern(item.name.as_str(), item.name.is_stropped())
                })
                .collect()
        })
        .unwrap_or_default();
    let function = fold
        .core
        .identities
        .mint_function(function_spelling, function_namespace);
    let resolved = ast_resolved::Relation::FunctorCall {
        alias: (),
        call: ast_resolved::SealedCall::from_inner(
            ast_resolved::FunctorCall {
                callee: function,
                arguments: crate::pipeline::asts::core::operators::CallArguments::higher_order(
                    resolved_ho_arguments,
                ),
                marks,
            },
            false,
        ),
    };

    // The access travels beside the call, in the position it was written:
    // after it, on what the call publishes.
    let authority = fold.core.identities.authority();
    let ast_resolved::Relation::FunctorCall { call, alias } = resolved else {
        unreachable!("the callable read was just built as a functor call")
    };
    let head = authority.reading(crate::relation::builder::ReadHead::Call {
        call,
        alias,
        published: schema,
    })?;
    ResolvedRelation::asking(head, resolved_spec, &fold.core.identities)
}

/// Resolve an InnerRelation variant (subquery inside parentheses).
///
/// INNER-RELATION: table(|> pipeline) or table(, correlation |> pipeline)
/// Resolves the subquery and keeps pattern as Indeterminate.
/// The refiner will classify it into UDT/CDT-SJ/CDT-GJ/CDT-WJ.
pub(super) fn resolve_inner_relation(
    rel: ast_unresolved::Relation,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
) -> Result<ResolvedRelation> {
    let ast_unresolved::Relation::InnerRelation {
        pattern,
        alias,
        outer,
        ..
    } = rel
    else {
        unreachable!("resolve_inner_relation called with non-InnerRelation variant");
    };

    // Extract identifier and subquery from the pattern
    let (identifier, subquery) = match pattern {
        ast_unresolved::InnerRelationPattern::Indeterminate {
            identifier,
            subquery,
            ..
        } => (identifier, subquery),
        _ => {
            return Err(crate::error::DelightQLError::ParseError {
                message: "Expected Indeterminate pattern from builder".to_string(),
                source: None,
                subcategory: None,
            });
        }
    };

    // Resolve the inner subquery, also collecting any pipe-level CFEs
    // from the sub-fold so the caller can propagate them to the outer fold.
    // The interior resolves under the access's self-name — the alias when
    // authored, the access name otherwise — so its spine stages keep that
    // name answering for the CURRENT heading.
    let interior_self = {
        let (text, stropped) = match &alias {
            Some(alias) => (alias.as_str(), alias.is_stropped()),
            None => (identifier.name.as_str(), identifier.name.is_stropped()),
        };
        fold.core.identities.intern(text, stropped)
    };
    let resolved_subquery = fold.resolve_interior((*subquery).clone())?;

    // A CORRELATED INNER RELATION PUBLISHES WHAT ITS CORRELATION READS.
    // Refinement hoists the correlation onto the enclosing join, and the
    // column it names has to survive a projection that did not mention it.
    // The carrier is injected HERE, under the boundary, because the boundary
    // derived below is what the outside addresses: injecting after it would
    // add a position the boundary stands over and cannot answer for.
    let correlation_filters = resolved_subquery.correlation_filters(&fold.core.identities)?;
    // THE INTERIOR IS SPENT HERE: its lexical extent ends at the boundary
    // below, which is what the outside addresses. The body travels; the
    // scope it answered under does not cross with it.
    let resolved_subquery = resolved_subquery.into_body();
    let resolved_subquery =
        crate::pipeline::refiner::pattern_classifier::inject_hygienic_columns_if_needed(
            resolved_subquery,
            &correlation_filters,
            &fold.core.identities,
        )?;

    // Create resolved InnerRelation with Indeterminate pattern; the refiner
    // classifies it later. The head's boundary — the effective name qualified
    // globs like `users.*` or `u.*` match through — is derived FROM the
    // subquery standing inside it, in the same act.
    let resolved = ast_resolved::Relation::InnerRelation {
        pattern: ast_resolved::InnerRelationPattern::Indeterminate {
            identifier: convert_qualified_name(identifier),
            subquery: Box::new(resolved_subquery),
        },
        alias,
        outer,
    };
    let head = fold.core.identities.authority().boundary_head(
        GroundForm::Reference(resolved),
        crate::relation::builder::Boundary::Alias {
            answer: interior_self,
        },
    )?;
    Ok(ResolvedRelation::answering_for_itself(
        ast_resolved::Chain::ground(head),
    ))
}

pub(crate) fn combine_where_constraints(
    constraints: Vec<ast_resolved::TruthExpression>,
) -> ast_resolved::TruthExpression {
    ast_resolved::TruthExpression::all(constraints)
        .expect("caller only combines a non-empty constraint list")
}

/// Relabel column metadata with an alias: if an alias is present, update the
/// table_name on each column to reflect the alias. Otherwise return a clone.
/// The relation a read stands on once an authored alias has replaced its
/// answering name, with the heading that alias publishes.
///
/// An alias REPLACES the answer, so the read's relation is the alias's; a
/// read with no alias continues to be the relation it named.
pub(crate) fn relabel_columns_with_alias(
    input: crate::relation::SemanticRelation,
    alias: &Option<SqlIdentifier>,
    identities: &crate::relation::Planning,
) -> Result<(
    crate::relation::SemanticRelation,
    Vec<crate::relation::PortId>,
)> {
    let Some(alias_name) = alias else {
        return Ok((input, crate::relation::published_ports(identities, &input)?));
    };
    let spelling = identities.intern(alias_name.as_str(), alias_name.is_stropped());
    let relation = identities
        .authority()
        .derive(crate::relation::RelForm::Export(
            crate::relation::form::ExportSpec {
                input,
                why: crate::relation::form::ExportWhy::Alias { answer: spelling },
            },
        ))?;
    let columns = crate::relation::published_ports(identities, &relation)?;
    Ok((relation, columns))
}
