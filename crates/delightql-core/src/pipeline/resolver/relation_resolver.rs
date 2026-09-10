// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Relation resolution logic
//!
//! This module handles the resolution of base relations and relational calls.
//! and pattern application for positional patterns.
use super::ResolvedRelation;
use crate::diagnostic::{Constraint, Internal, Narrowing, Resolution, Runtime, Semantic};
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
        return Err(Internal::invariant(
            "resolver::relation_resolver",
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
        ast_unresolved::Access::Slots(_) => Err(DelightQLError::from(Resolution::Schema {
            message: "A positional relation access requires a resolved heading".to_string(),
        })),
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
        Internal::invariant(
            "resolver::relation_resolver",
            "A structural relation was read before its binding was resolved",
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
        return Err(Internal::invariant(
            "resolver::relation_resolver",
            "A plan-scope relation was read before its heading was published",
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
            return Err(Internal::invariant(
                "resolver::relation_resolver",
                "A direct plan-scope read cannot carry user access metadata",
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

/// Resolve a Ground relation variant (named table, view, CTE, or consulted
/// entity) in HEAD position: select once, then open the answer whole.
pub(super) fn resolve_ground(
    rel: ast_unresolved::Relation,
    access: ast_unresolved::Access,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
) -> Result<ResolvedRelation> {
    select_ground(rel, fold)?.into_whole(access, fold)
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
        Runtime::poisoned(
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
    Internal::invariant("bootstrap_serving", message)
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
            return Err(DelightQLError::from(
                Semantic::MaterializationBootstrapBlob {
                    message: "a bootstrap BLOB column has no literal spelling to serve".to_string(),
                },
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
            return DelightQLError::from(Resolution::FactFunctionRelationalFace {
                message: format!(
                    "'{name}' is a default-bearing fact function and has no relational face — \
                     call it as `{name}:(inputs)`, or map that call over a separately supplied \
                     finite relation"
                ),
            });
        }
        BootstrapEntityType::DqlFunctionExpression
        | BootstrapEntityType::DqlHoFunctionExpression
        | BootstrapEntityType::DqlContextAwareFunctionExpression => format!(
            "'{name}' is a function, not a relation — call it as \
             `{name}:(args)`. (A case/scalar function has no relation face \
             `{name}(*)`.)"
        ),
        BootstrapEntityType::DqlHoTemporaryViewExpression => {
            // A higher-order view invoked without its relation argument is
            // an arity refusal, not an unresolved name.
            return DelightQLError::from(crate::diagnostic::Semantic::Arity {
                message: format!(
                    "'{name}' is a higher-order view, not a relation — supply its \
                     relation argument, for example `{name}(source(*))(*)`"
                ),
            });
        }
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
    DelightQLError::from(Resolution::General {
        message: message.to_string(),
    })
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
    DelightQLError::from(Resolution::General {
        message: format!(
            "'{name}' ({entity_type:?}) is a bin relation served by the runtime, and this \
             occurrence escaped the executable boundary that produces its \
             rows — a compiler fence, not a semantic outcome; the direct, \
             bound and consulted spellings all execute"
        ),
    })
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
        return Err(DelightQLError::from(Constraint::General {
            message:
                "Passthrough table access requires a namespace path (e.g., main/table_name(*))"
                    .to_string(),
        }));
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
        return Err(DelightQLError::from(Resolution::Schema {
    message: format!(
                "Passthrough table '{}/{}' schema not available — only (*) is allowed, not positional binding",
                identifier.namespace_path, identifier.name
            ),
}));
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
/// THE SCOPE A LEXICAL RELATION NAME READS: a query-local CTE or a
/// plan-created relation, as the one access boundary publishes it.
///
/// Both positions that may read such a name — a chain head and a join
/// member — derive it here and nowhere else. A read built from the
/// binding's own body relation instead answers to whatever that body's
/// source was bound to, so the member spelling would name the CTE's first
/// table where the head spelling names the CTE.
fn local_read_scope(
    entity_info: crate::resolution::EntityInfo,
    frontier: Option<crate::defuse::instance::DefinitionFrontier>,
    identifier: &ast_unresolved::QualifiedName,
    alias: Option<&SqlIdentifier>,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
) -> Result<crate::relation::SemanticRelation> {
    use crate::resolution::EntityDefinition;

    if let Some(frontier) = &frontier {
        crate::defuse::bound_use::judge_recursive_frontier(&fold.config.instances, frontier)?;
    }

    let canonical_name = entity_info.canonical_name.clone();
    let backend_schema = entity_info.backend_schema;
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
        return Err(Internal::invariant(
            "resolver::relation_resolver",
            "Plan scratch must be referenced by scope identity",
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
    let access_name: SqlIdentifier = alias.cloned().unwrap_or_else(|| identifier.name.clone());
    let access_spelling = fold
        .core
        .identities
        .intern(access_name.as_str(), access_name.is_stropped());
    // ONE INSTANCE of the binding: the access boundary is its own relation,
    // and what crosses it, under what name, is the boundary's law.
    fold.core
        .identities
        .authority()
        .derive(crate::relation::RelForm::Instantiate(
            crate::relation::form::InstanceSpec {
                kind: crate::relation::form::DefinitionKind::Cte,
                template: cte_schema,
                answers_to: Some(access_spelling),
            },
        ))
}

/// THE SCOPE A CATALOG RELATION NAME READS, with its physical identity
/// bound and its alias export derived. Shared by the head and member
/// positions for the same reason [`local_read_scope`] is.
fn catalog_read_scope(
    entity_info: crate::resolution::EntityInfo,
    alias: &Option<SqlIdentifier>,
    access: &ast_unresolved::Access,
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
) -> Result<crate::relation::SemanticRelation> {
    use crate::resolution::EntityDefinition;

    let canonical_name = entity_info.canonical_name.clone();
    let entity_backend_schema = entity_info.backend_schema;
    let EntityDefinition::RelationSchema(table_schema) = entity_info.definition;
    bind_physical_relation(
        table_schema,
        canonical_name.as_ref(),
        entity_backend_schema.as_deref(),
        &fold.core.identities,
    )?;
    // THE ALIAS REPUBLICATION IS THE WHOLE READ'S. A read that names no
    // dimensions answers to its alias by republishing the base heading
    // under it. A SLOT ROW publishes its own interface instead, owned by
    // the same alias through the pattern owner — republishing underneath
    // it re-roots the base ports, and a ground slot's restriction then
    // names a port the emitted read no longer binds.
    if matches!(access, ast_unresolved::Access::Slots(_)) {
        return Ok(table_schema);
    }
    let (aliased, _base_cols) =
        relabel_columns_with_alias(table_schema, alias, &fold.core.identities)?;
    Ok(aliased)
}

/// WHAT A NAMED GROUND MENTION DENOTES — selected ONCE.
///
/// The lookup runs here and nowhere else. The answer then travels to
/// whichever finish the position needs: a chain head takes the whole
/// relation, a slotted join member takes an unfinished read where the
/// answer is a readable scope. No outcome carries permission to look the
/// spelling up again, so one use never selects twice and the two positions
/// cannot disagree about what a name means.
pub(super) struct GroundSelection<'db> {
    identifier: ast_unresolved::QualifiedName,
    alias: Option<SqlIdentifier>,
    outer: bool,
    /// `!!` is evidence about the relation this access reads, and it
    /// belongs to the occurrence the access publishes.
    marked_relation: Option<crate::names::Spelling>,
    denotes: Denotation<'db>,
}

/// What the selection found. A passthrough carries no catalog answer: its
/// own backend lookup needs the access, so the finish performs it — once.
enum Denotation<'db> {
    Passthrough,
    Catalog {
        answer: crate::defuse::environment::RelationAnswer<'db>,
        serve: Option<ServedBootstrapRead>,
    },
}

/// A JOIN MEMBER'S OUTCOME, from the one selection it spent.
pub(super) enum MemberOutcome {
    /// The slot row applied to the selected readable scope. The
    /// constraints are still the join's to partition into its correlation
    /// and the read's own restriction, which is the only thing the member
    /// position decides.
    Patterned(super::lexical::PatternRead),
    /// The selection opened whole. Not a permission to select again — this
    /// relation IS the answer this member's selection produced.
    Whole(ResolvedRelation),
}

/// SELECT what a named ground mention denotes. One lookup, both spellings:
/// the environment owns the closed qualified decision (catalog provider,
/// current definitions, closed miss) exactly as it owns the unqualified
/// ladder.
pub(super) fn select_ground<'db>(
    rel: ast_unresolved::Relation,
    fold: &mut super::resolver_fold::ResolverFold<'_, 'db>,
) -> Result<GroundSelection<'db>> {
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
        unreachable!("select_ground called with a mention that is not a name");
    };

    // Recorded before anything is built on it, so every relation built from
    // that occurrence afterwards carries the evidence — a name, an alias, a
    // CTE binding or a join arm hands it on instead of leaving a later
    // reader to walk the syntax back to a ground name it may no longer have.
    let marked_relation = mutation_target.then(|| {
        fold.core
            .identities
            .intern(identifier.name.as_str(), identifier.name.is_stropped())
    });

    let denotes = if passthrough {
        // PASSTHROUGH: skip the entity catalog; the schema introspector
        // answers, and it answers with the access in hand.
        Denotation::Passthrough
    } else if identifier.namespace_path.is_empty() {
        Denotation::Catalog {
            answer: fold
                .env
                .relation(fold.core, &identifier.name, alias.as_ref())?,
            serve: None,
        }
    } else {
        let (answer, serve) = fold.env.relation_qualified(
            fold.core,
            &identifier.namespace_path,
            &identifier.name,
            fold.config.serve_bootstrap_reads,
        )?;
        Denotation::Catalog {
            answer,
            serve: serve.map(|serve| ServedBootstrapRead {
                canonical: serve.canonical,
                backend_schema: serve.backend_schema,
                namespace_fq: serve.namespace_fq,
            }),
        }
    };

    Ok(GroundSelection {
        identifier,
        alias,
        outer,
        marked_relation,
        denotes,
    })
}

impl<'db> GroundSelection<'db> {
    /// The WHOLE relation this selection denotes.
    pub(super) fn into_whole(
        self,
        access: ast_unresolved::Access,
        fold: &mut super::resolver_fold::ResolverFold<'_, 'db>,
    ) -> Result<ResolvedRelation> {
        use crate::defuse::environment::RelationAnswer;

        let GroundSelection {
            identifier,
            alias,
            outer,
            marked_relation,
            denotes,
        } = self;

        let (answer, serve) = match denotes {
            Denotation::Passthrough => {
                let resolved = r_resolve_passthrough(identifier, access, alias, outer, fold)?;
                resolved.noting_mutation_mark(marked_relation, &fold.core.identities)?;
                return Ok(resolved);
            }
            Denotation::Catalog { answer, serve } => (answer, serve),
        };

        let resolved = match answer {
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
            // THE CATEGORY IS RIGHT AND THE ROAD IS MISSING. Reaching this
            // arm means the executable boundary — which runs before
            // resolution, over the submission's own chains — did not see
            // this occurrence, so the rows were never produced. Refusing
            // here is what keeps a known relation out of the generic-TVF
            // fallback, where its namespace would be stripped and SQL
            // generated against a table that does not exist.
            RelationAnswer::RuntimeServedRelation { name, entity_type } => {
                Err(runtime_served_unreached_error(&name, entity_type))
            }
            RelationAnswer::Ambiguous(message) => {
                Err(DelightQLError::from(Resolution::Ambiguous {
                    message: message.to_string(),
                }))
            }
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
        match serve {
            Some(served) => serve_bootstrap_relation(resolved, served, fold),
            None => Ok(resolved),
        }
    }

    /// The SLOTTED MEMBER'S outcome. A readable scope keeps its read
    /// unfinished for the join; every other answer this selection produced
    /// opens whole, through the very same finish the head position uses.
    pub(super) fn into_member(
        self,
        access: &ast_unresolved::Access,
        fold: &mut super::resolver_fold::ResolverFold<'_, 'db>,
    ) -> Result<MemberOutcome> {
        use crate::defuse::environment::RelationAnswer;

        // A mutation mark and a served bootstrap read are acts on a
        // FINISHED relation — the mark rides the occurrence the access
        // publishes, the serving replaces the read with its own literal
        // scope — so this selection opens whole for them. Neither looks the
        // name up again.
        let readable = match &self.denotes {
            Denotation::Passthrough => false,
            Denotation::Catalog { serve, answer } => {
                self.marked_relation.is_none()
                    && serve.is_none()
                    && matches!(
                        answer,
                        RelationAnswer::CTE { .. }
                            | RelationAnswer::MaterializedRelation(_)
                            | RelationAnswer::DatabaseEntity(_)
                    )
            }
        };
        if !readable {
            return Ok(MemberOutcome::Whole(self.into_whole(access.clone(), fold)?));
        }

        let GroundSelection {
            identifier,
            alias,
            outer,
            denotes,
            ..
        } = self;
        let Denotation::Catalog { answer, .. } = denotes else {
            unreachable!("a readable member selection carries a catalog answer");
        };
        let scope = match answer {
            RelationAnswer::CTE { entity, frontier } => {
                local_read_scope(entity, frontier, &identifier, alias.as_ref(), fold)?
            }
            RelationAnswer::MaterializedRelation(entity) => {
                local_read_scope(entity, None, &identifier, alias.as_ref(), fold)?
            }
            RelationAnswer::DatabaseEntity(entity) => {
                catalog_read_scope(entity, &alias, access, fold)?
            }
            _ => unreachable!("the readable judgment admitted only these three answers"),
        };

        // THE ONE ARGUMENTATIVE OPERATION, over the relation the selection
        // answered with: the slot row is judged and applied there, and the
        // read comes back unfinished for the join that owns the left row.
        Ok(MemberOutcome::Patterned(ResolvedRelation::patterned(
            super::PatternOperand::Read { scope, outer },
            access,
            pattern_owner(&alias),
            fold,
        )?))
    }
}

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
    let instance = local_read_scope(entity_info, frontier, &identifier, alias.as_ref(), fold)?;
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
    let aliased = catalog_read_scope(entity_info, &alias, &access, fold)?;
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

    Err(DelightQLError::from(Resolution::Table {
        table: table_name,
        context,
    }))
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
pub(in crate::pipeline::resolver) fn infer_anon_column_types(
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

pub(in crate::pipeline::resolver) fn infer_anon_column_shapes(
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
        return Err(DelightQLError::from(Narrowing::ObjectLiteral {
            message: format!(
                "narrowing iterates an array — every row of '{column}' is a single \
                 object. Path into the object instead: ({column}:{{.field}}), or \
                 spell the one-element sequence: [{{...}}]."
            ),
        }));
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

    // THE HEADER IS A SLOT ROW, judged by the lexical authority over the
    // row this relation is composed with — bind, reuse, ground, disregard,
    // the same vocabulary the caller pattern reads — and BORN by the same
    // judgment: the carrier that judged it owns the alias, the planning
    // authority, the grid's facts and every reuse edge, and its one
    // consuming operation derives the relation. Only binders publish: a
    // repeated binder is the same variable twice (one published column and
    // an equality between the positions), `_` disregards, and a ground or
    // computed term constrains and publishes nothing. This road supplies
    // the birth one thing: how a computed header resolves over the row.
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
    let (header_values, resolved_schema) = if let Some(header) = &header {
        let planning: &crate::relation::Planning = fold.core.identities;
        let birth = fold.lexical.judge_anonymous_header(
            header,
            &rows,
            &resolved_rows,
            relation_alias.as_ref(),
            planning,
        )?;
        // A computed header names a column of the ENCLOSING row —
        // `_(upper:(description) @ …)` probes the outer relation's
        // `description`. It resolves against the same context the data
        // rows do, because it is a reference out of the same place; the
        // birth asks for each such term in position order.
        let born = birth.born(|term| resolve_against_outer_context(fold, term))?;
        let mut values = Vec::with_capacity(born.positions.len());
        for position in born.positions {
            match position {
                super::BornPosition::Binds => {
                    roles.push(HeaderRole::Binder);
                    values.push(None);
                }
                super::BornPosition::Repeats { first } => {
                    roles.push(HeaderRole::Repeat { first });
                    // The value is written below, from the first binder's
                    // port.
                    values.push(None);
                }
                super::BornPosition::Disregards => {
                    roles.push(HeaderRole::Disregard);
                    values.push(None);
                }
                super::BornPosition::Constrains(resolved) => {
                    roles.push(HeaderRole::Constrains);
                    values.push(Some(resolved));
                }
            }
        }
        (Some(values), born.relation)
    } else {
        // A headerless grid: one inferred position per column, reusing
        // nothing — the plain road.
        let inferred_types = infer_anon_column_types(&resolved_rows);
        let inferred_shapes = infer_anon_column_shapes(&resolved_rows);
        let inferred_slots: Vec<crate::relation::form::AnonymousSlot> =
            (0..resolved_rows.first().len())
                .map(|idx| crate::relation::form::AnonymousSlot::Inferred {
                    position: idx as u32,
                    declared_type: inferred_types.get(idx).cloned().flatten(),
                    shape: inferred_shapes.get(idx).copied().unwrap_or_default(),
                })
                .collect();
        (
            None,
            fold.core
                .identities
                .authority()
                .derive(crate::relation::RelForm::Anonymous(
                    crate::relation::form::AnonymousSpec::plain(
                        crate::relation::form::AnonymousShape::Tabular,
                        &inferred_slots,
                        scope_answer,
                    ),
                ))?,
        )
    };
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
            .map(|(port, _)| crate::relation::pending::Position::restating(*port, None))
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
    // no index for a copy to disagree with, and no second copy of the
    // flowing relation beside the row. That road admits the row against
    // the declaration it selects.

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
                DelightQLError::from(Resolution::General {
                    message: format!("invalid namespace on relation '{}': {:?}", function, error),
                })
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
                let resolved = fold
                    .lexical
                    .flatly(|position| position.address(reference, false, &fold.core.identities))?;
                let occurrence = match resolved {
                    UnificationResult::Resolved(occurrence) => occurrence,
                    UnificationResult::Unresolved(column) => {
                        return Err(DelightQLError::from(Resolution::Column {
    column: column.to_string(),
    context: "in TVF argument".to_string(),
}))
                    }
                    UnificationResult::Ambiguous { column, tables } => {
                        return Err(DelightQLError::from(Resolution::Ambiguous {
    message: format!(
                                "Column '{column}' in TVF argument is ambiguous. Could refer to: {}",
                                tables.join(", ")
                            ),
}))
                    }
                    UnificationResult::Opaque => {
                        return Err(crate::pipeline::resolver::opaque_reference_refusal())
                    }
                    UnificationResult::Refused(refusal) => return Err(refusal),
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
                let resolved = fold
                    .lexical
                    .flatly(|position| position.address(reference, false, &fold.core.identities))?;
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
                        return Err(DelightQLError::from(Resolution::Column {
                            column: column.to_string(),
                            context: "in TVF argument".to_string(),
                        }))
                    }
                    UnificationResult::Refused(refusal) => return Err(refusal),
                    UnificationResult::Ambiguous { column, tables } => {
                        return Err(DelightQLError::from(Resolution::Ambiguous {
                            message: format!(
                                "Ambiguous column '{}' exists in scopes: {}",
                                column,
                                tables.join(", ")
                            ),
                        }))
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

    // A DIRECTIVE NEVER REACHES THE TARGET-FUNCTION ROAD. Every `!`-named
    // callee is the routing authority's: a built-in is realized by its
    // descriptor and a user directive by its definition, and either standing
    // here means classification let an effect through as a pure call. Only
    // a genuinely unknown, non-directive name keeps the open fallback.
    if function.ends_with('!') {
        return Err(Internal::invariant(
            "resolver::relation_resolver",
            format!(
                "directive '{function}' reached target-function resolution; the routing \
                 authority owns every directive callee"
            ),
        ));
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
            return Err(DelightQLError::from(Resolution::CallableUnknown {
                message: format!("Unknown TVF: {function}"),
            }));
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
                    return Err(DelightQLError::from(Constraint::General {
                        message: format!(
                            "the heading of TVF '{}' is an ordered projection of \
                             the function's columns — each slot must be a bare \
                             column name",
                            table_name
                        ),
                    }));
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
                    return Err(DelightQLError::from(Constraint::General {
                        message: format!(
                            "Duplicate column '{name}' in the heading of TVF \
                             '{table_name}': programmer-authored names must be \
                             unique. Rename one with 'as' to disambiguate"
                        ),
                    }));
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
                        return Err(DelightQLError::from(Resolution::Column {
                            column: name.to_string(),
                            context: format!("in the heading of TVF '{}'", table_name),
                        }))
                    }
                    (Some(_), Some(_)) => {
                        return Err(DelightQLError::from(Resolution::Ambiguous {
                            message: format!(
                                "TVF '{table_name}' publishes '{name}' more than \
                                 once, so a heading slot naming it reaches no \
                                 single column"
                            ),
                        }))
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
                            return Err(DelightQLError::from(Constraint::General {
                                message: format!(
                                    "Duplicate column '{}' in the declared heading of \
                                 '{table_name}': programmer-authored names must be \
                                 unique. Rename one with 'as' to disambiguate",
                                    name
                                ),
                            }));
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
                        crate::relation::form::AnonymousSpec::plain(
                            crate::relation::form::AnonymousShape::Tabular,
                            &declared_slots,
                            Some(hint),
                        ),
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
                return Err(DelightQLError::from(Resolution::CallableUnknown {
                    message: format!(
                        "no DQL callable '{function}' exists, and the default target \
                         transpilation cannot carry its relation or callable \
                         argument — a target function row holds values only. \
                         Every authored argument must survive the fallback, so the \
                         call refuses instead of dropping one."
                    ),
                }));
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
            return Err(Internal::invariant(
                "resolver::relation_resolver",
                "Expected Indeterminate pattern from builder".to_string(),
            ));
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
    // THE INTERIOR IS EVALUATED AT THE ENCLOSING JOIN. Its correlations are
    // hoisted there, so every restriction inside that reads the enclosing
    // row is the correlation act: the relation it stands on owes the
    // interior occurrences it reads, and each operation up to the boundary
    // keeps them readable or refuses. Nothing is injected afterwards and
    // nothing later rediscovers what was owed.
    let resolved_subquery = fold.resolve_interior(
        (*subquery).clone(),
        crate::pipeline::resolver::Correlations::Hoisted,
    )?;
    // THE INTERIOR IS SPENT HERE: its lexical extent ends at the boundary
    // below, which is what the outside addresses. The body travels; the
    // scope it answered under does not cross with it.
    let resolved_subquery = resolved_subquery.into_body();

    // Create resolved InnerRelation with Indeterminate pattern; the refiner
    // classifies it later. The head's boundary — the effective name qualified
    // globs like `users.*` or `u.*` match through — is derived FROM the
    // subquery standing inside it, in the same act, and it is where the
    // body's correlation support is spent outward.
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
        crate::relation::builder::Boundary::Interior {
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
