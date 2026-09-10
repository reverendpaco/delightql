// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The PARAMETERIZED/HIGHER-ORDER use road of the definition-use
//! authority: one entrance selects the family, binds the caller-resolved
//! actuals to the declared formals (the final-landing law included), admits
//! the instance under the semantic actual key, opens the body, squishes
//! it with the bindings, and resolves it in the family's own declaration
//! environment. Everything below the entrance is private machinery — no
//! production caller can select an HO family and compose the steps
//! itself.

use crate::diagnostic::{Grounding, Ho, HoDefinition, Internal, Recursion, Resolution, Semantic};
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::{AuthoredColumn, NamedReference, Reference};
use crate::pipeline::asts::core::{ColumnOccurrence, Comparison, Step};
use crate::pipeline::asts::ddl::{Clause, HeadItem, HoParam, HoPositionInfo};
use crate::pipeline::asts::vocabulary::Vec1;
use crate::pipeline::query_features::HoParamBindings;
use crate::pipeline::resolver::relation_resolver::combine_where_constraints;
use crate::pipeline::resolver::ResolvedRelation;
use delightql_types::SqlIdentifier;

use super::application::{Actual, Application, ParamRowCompletion};

/// Unified HO view expansion: handles both direct and piped invocations.
///
/// The first parens (scalar params) are one slot row over the squished
/// relation, which includes ALL clauses; the row's constraints from
/// call-site literals restrict it, never a per-clause pre-filter.
///
/// THE ONE EXPANSION ROAD for a selected parameterized family:
///
/// 1. the caller's relation carriers RESOLVE IN THE CALLER'S WORLD;
/// 2. the caller's scalar actuals RESOLVE IN THE CALLER'S WORLD (over the
///    caller row and the resolved carriers) into the body's formal frame;
/// 3. `use_closed_ho` admits the use under the resolved semantic key
///    (or re-enters the fixpoint BY ITS FRONTIER'S IDENTITY);
/// 4. `HoUse::resolve_squished` spends the admitted use in the family's
///    own world and returns the finished expansion artifact;
/// 5. the call-site scalar spec filters and the trailing access group bind
///    over the RESOLVED result.
#[allow(clippy::too_many_arguments)]
fn expand_ho_view(
    function: &str,
    selection: HoSelection,
    scalar_spec: &ast_unresolved::Access,
    access_spec: &ast_unresolved::Access,
    mut table_bindings: crate::pipeline::query_features::HoParamBindings,
    scalar_actuals: Vec<(String, ast_unresolved::DomainExpression)>,
    rule_actuals: std::collections::HashMap<SqlIdentifier, RuleValueId>,
    residual_row_tokens: &[crate::relation::PortId],
    seed: Option<super::bound_use::HoActuals>,
    prepared_pipe: Option<crate::defuse::carriers::CarrierRecord>,
    scoped_world: Option<super::environment::ClosedLexicalWorld>,
    caller_row: &mut crate::pipeline::resolver::CallerRow,
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold<'_, '_>,
    user_alias: Option<SqlIdentifier>,
) -> Result<ResolvedRelation> {
    let output_is_glob = matches!(
        selection.output()?,
        crate::pipeline::asts::core::definitions::HeadItems::Glob
    );
    let seed_bindings = seed.as_ref().map(|seed| seed.bindings.clone());
    if let Some(prefix) = seed_bindings {
        merge_ho_bindings(&mut table_bindings, prefix)?;
    }
    // A SELF-REFERENCE ADMITS NO CALLER ROW. The family being expanded is
    // open in the instance table before its body resolves, so a use of it
    // from inside that body is a re-entry (or a widening, or a cycle),
    // never a fresh expansion: it reads the frontier, whose rows carry the
    // instance's own actuals, and the row standing beside it — a member
    // the clause joined before the self-read — stays standing for the
    // enclosing join, exactly as it would beside any other relation.
    let reentering = match &selection {
        HoSelection::Family(family) => family.is_open(&fold.config.instances),
        HoSelection::Scoped(_) => false,
    };
    // THE PIPE IS A FACT OF THE BOUND FORMALS: the split bound its landed
    // member as a carrier, and the formal it landed at is recorded by that
    // bind — no second copy of the flowing relation travels beside the row.
    let absorbs_input =
        table_bindings.formals.landed_source().is_some() || (caller_row.stands() && !reentering);

    // THE AUTHORED-SYNTAX FACTS of the scalar actuals, judged in the
    // caller before anything resolves: which params the caller supplied as
    // a BARE NAME (the dispatch-witness and shadowing judgments), and
    // which as a literal (the glob call-site hygiene judgment).
    let mut authored_bare = seed
        .as_ref()
        .map(|seed| seed.authored_bare.clone())
        .unwrap_or_default();
    for (param, expr) in &scalar_actuals {
        match expr {
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn {
                    name,
                    qualifier: None,
                    ..
                },
            ))) => {
                authored_bare.insert(param.clone(), name.to_string());
            }
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Ground(_),
            ) => {}
            _ => {}
        }
    }

    // A call-site lvar makes the caller input a structural carrier inside
    // every clause, where free heads bind it and ground heads constrain it.
    let has_free_scalars = !authored_bare.is_empty();
    let caller_scalar_relation = if has_free_scalars {
        caller_row
            .standing_relation(&fold.lexical)
            .map(|row| row.semantic_relation())
    } else {
        None
    };
    let join_bound_scalars: std::collections::HashSet<_> = caller_scalar_relation
        .as_ref()
        .map(|_| authored_bare.keys().cloned().collect())
        .unwrap_or_default();
    let prepared_join_input = prepared_pipe
        .as_ref()
        .and_then(|prepared| prepared.join_input().or(prepared.absorbed_join_input()));
    let join_input_cte = if prepared_join_input.is_some() {
        // A configured residual already captured this exact caller row and
        // crossed the resulting carrier into the receiving use. The capture
        // stands for the row, so this site absorbs it: resolving the caller
        // row again would create a second evaluation row.
        caller_row.absorb(&mut fold.lexical);
        None
    } else if has_free_scalars && !reentering {
        // A call-site lvar makes the caller row a structural carrier inside
        // every clause. Taking it IS recording that the enclosing join must
        // not add it again.
        caller_row.absorb(&mut fold.lexical)
    } else {
        None
    };

    // THE RELATION FORMALS WERE BOUND where the row was admitted; what
    // remains to bind here is the caller row the call absorbed.
    let mut carriers = super::carriers::bind_join_input(fold, join_input_cte)?;
    if let Some(mut prepared) = prepared_pipe {
        prepared.absorb(carriers);
        carriers = prepared;
    }
    if let Some(seed) = seed.as_ref() {
        // The residual keeps its closed semantic bindings, while every
        // physical definition it owns remains ahead of the body that reads
        // it. Exact CTE scope identity deduplicates nested reuse during SQL
        // cleanup; ambient structural registrations do not reconstruct
        // ownership here.
        carriers.seeded_by(&seed.carriers);
    }
    // THE SCALAR ACTUALS RESOLVE IN THE CALLER'S WORLD. Expressions can read
    // the caller's row and resolved carrier headings, but a bare actual is
    // stricter: only an exact formal binding, the exact standing row, or the
    // exact relation landed by a pipe supplies it. A sibling relation actual
    // is another argument, not the caller row. What crosses is the RESOLVED
    // value, as the formal frame.
    let mut frame: std::collections::HashMap<
        delightql_types::SqlIdentifier,
        crate::pipeline::asts::resolved::DomainExpression,
    > = seed
        .as_ref()
        .map(|seed| seed.values.clone())
        .unwrap_or_default();
    if !scalar_actuals.is_empty() {
        // THE CARRIERS ARE THE ROW A SCALAR ACTUAL STANDS OVER: a joined or
        // piped input republishes the caller's columns under the carrier's
        // own ports, and a bare-lvar actual denotes the carrier's column —
        // the carriers are the frame, and the caller's row encloses it, so
        // a name the carriers publish shadows the outer occurrence of the
        // same name while outer columns under other names stay reachable.
        for (param, expr) in scalar_actuals {
            // A bare actual resolves against an exact caller identity: a
            // frame-local formal, the standing row beside a join, or the
            // relation landed by a pipe. The formal lookup spends the private
            // identity issued by its frame in a CLOSED position. Sibling
            // columns with the same spelling cannot become the actual or
            // make that occurrence ambiguous.
            let mut actual_fold = if authored_bare.contains_key(&param) {
                let is_exact_formal = match &expr {
                    ast_unresolved::DomainExpression::Reference(Reference::Named(
                        NamedReference(authored),
                    )) if authored.qualifier.is_none() && authored.namespace_path.is_empty() => {
                        fold.env.formal_value(&authored.name).is_some()
                    }
                    _ => false,
                };
                if is_exact_formal {
                    fold.child_closed()
                } else if reentering {
                    // A self-reference took no carrier: a bare actual that
                    // is not the instance's own formal is a different
                    // actual, judged by the admission below as widening.
                    fold.child(crate::pipeline::resolver::Correlations::InPlace)
                } else {
                    // THE CARRIER THE CALLER ROW BECAME, as a stage: the
                    // record names it, and the frame is minted from its
                    // receipt.
                    let mut actual_fold =
                        fold.child(crate::pipeline::resolver::Correlations::InPlace);
                    if !actual_fold
                        .lexical
                        .enter_landing(&carriers, &actual_fold.core.identities)?
                    {
                        return Err(incomplete_scalar_actual(function, &param));
                    }
                    actual_fold
                }
            } else {
                // THE CARRIERS ARE THE ROW A SCALAR ACTUAL STANDS OVER: the
                // record of this call's carriers, framed as one row from its
                // own receipts.
                let mut actual_fold = fold.child(crate::pipeline::resolver::Correlations::InPlace);
                actual_fold
                    .lexical
                    .enter_carriers(&carriers, &actual_fold.core.identities)?;
                actual_fold
            };
            // THE ACTUAL IS EVALUATED WHERE THE CALL STANDS, inside the
            // statement the call's level emits: one reading past the
            // interior boundary the enclosing join evaluates has no value
            // there and refuses.
            let resolved = match actual_fold
                .judged_here("a scalar actual inside the interior", |fold| {
                    fold.transform_domain(expr)
                }) {
                Ok(resolved) => resolved,
                Err(error)
                    if authored_bare.contains_key(&param)
                        && matches!(
                            error,
                            DelightQLError::Semantic(Semantic::Resolution(
                                Resolution::Column { .. }
                            ))
                        ) =>
                {
                    return Err(incomplete_scalar_actual(function, &param));
                }
                Err(error) => return Err(error),
            };
            // A caller-resolved LITERAL is also a VALUE fact for the one
            // pre-resolution consumer (a row bound): literal provenance
            // threads through NESTED expansions, so a formal handed down a
            // level still bounds the inner body.
            if let crate::pipeline::asts::resolved::DomainExpression::Application(
                crate::pipeline::asts::resolved::FunctionApplication::Ground(value),
            ) = &resolved
            {
                table_bindings
                    .scalar_literals
                    .entry(param.clone())
                    .or_insert_with(|| value.clone());
            }
            frame.insert(delightql_types::SqlIdentifier::new(param), resolved);
        }
    }
    let mut rules = seed.map(|seed| seed.rules).unwrap_or_default();
    rules.extend(rule_actuals);
    let actuals = super::bound_use::HoActuals {
        carriers,
        bindings: table_bindings,
        values: frame,
        authored_bare: authored_bare.clone(),
        rules,
    };

    // THE ONE DEFINITION-USE ENTRANCE owns admission and opening: the
    // caller-resolved actuals serialize to the semantic key, the instance
    // installs BEFORE the body opens, widening refuses terminally inside
    // the entrance, and a same-key self-reference re-enters the fixpoint
    // THROUGH ITS FRONTIER — by identity, never by spelling. A CHOE is
    // admitted under its query-local identity and has no fixpoint: its
    // self-reference refuses.
    let opened = match selection {
        HoSelection::Family(family) => super::admitted::use_closed_ho(
            &fold.config.instances,
            family,
            function,
            actuals,
            absorbs_input,
        )?,
        HoSelection::Scoped(definition) => {
            let opened = super::bound_use::use_scoped_ho(
                &fold.config.instances,
                definition,
                function,
                actuals,
            )?;
            let positions = opened.positions().to_vec();
            let expansion = opened.resolve_squished(
                function,
                scalar_spec,
                scoped_world,
                residual_row_tokens,
                fold,
            )?;
            return finish_ho_expansion(
                function,
                positions,
                expansion,
                scalar_spec,
                access_spec,
                authored_bare,
                join_bound_scalars,
                output_is_glob,
                fold,
                user_alias,
            );
        }
    };
    let opened = match opened {
        super::bound_use::HoUseOutcome::Open(opened) => opened,
        super::bound_use::HoUseOutcome::Reenter { frontier } => {
            let reachable = frontier.as_ref().and_then(|frontier| {
                fold.env
                    .frontier_relation(frontier)
                    .map(|relation| (frontier.clone(), relation))
            });
            let Some((frontier, relation)) = reachable else {
                return Err(DelightQLError::from(Recursion::ConsultedClauseOrder {
                    message: format!(
                        "circular consulted-definition expansion: '{function}' is \
                         already being expanded and its fixpoint binding is not \
                         reachable from this position. If this is a recursive \
                         rule, the base (non-recursive) clause must come FIRST; \
                         if the cycle runs through another definition, break the \
                         cycle. SEMANTICS/recursion-contract-law.md B5."
                    ),
                }));
            };
            let entity_info = crate::resolution::EntityInfo {
                name: frontier.name().clone(),
                canonical_name: None,
                resolved_namespace: None,
                backend_schema: None,
                entity_type: crate::resolution::ResolvedEntityKind::Relation,
                registry_source: crate::resolution::RegistrySource::QueryLocal,
                schema_source: crate::resolution::SchemaSource::SelectClause,
                definition: crate::resolution::EntityDefinition::RelationSchema(relation),
            };
            let identifier = ast_unresolved::QualifiedName {
                namespace_path: ast_unresolved::NamespacePath::empty(),
                name: frontier.name().clone(),
            };
            let resolved = crate::pipeline::resolver::relation_resolver::r_resolve_cte(
                entity_info,
                Some(frontier),
                identifier,
                access_spec.clone(),
                user_alias,
                false,
                fold,
            )?;
            return Ok(resolved);
        }
    };
    let positions = opened.positions().to_vec();

    // THE CONSUMING OPERATION owns clause shaping, the body's world, and
    // resolution: this road hands over the resolved carriers and receives
    // the RESOLVED body.
    let expansion = opened.resolve_squished(function, scalar_spec, residual_row_tokens, fold)?;
    finish_ho_expansion(
        function,
        positions,
        expansion,
        scalar_spec,
        access_spec,
        authored_bare,
        join_bound_scalars,
        output_is_glob,
        fold,
        user_alias,
    )
}

/// The expansion's CALL-SITE HALF, over the RESOLVED body — the same for
/// a consulted family and a CHOE: the hygienic-binder judgment, the
/// call-site scalar spec, and the trailing access group.
#[allow(clippy::too_many_arguments)]
fn finish_ho_expansion(
    function: &str,
    positions: Vec<HoPositionInfo>,
    expansion: super::bound_use::SquishedExpansion,
    scalar_spec: &ast_unresolved::Access,
    access_spec: &ast_unresolved::Access,
    authored_bare: std::collections::HashMap<String, String>,
    join_bound_scalars: std::collections::HashSet<String>,
    output_is_glob: bool,
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold<'_, '_>,
    user_alias: Option<SqlIdentifier>,
) -> Result<ResolvedRelation> {
    let super::bound_use::SquishedExpansion {
        resolved: resolved_query,
        actuals: spent_actuals,
    } = expansion;

    // Convert to ConsultedView relation
    let resolved_expr = crate::pipeline::resolver::relation_resolver::view_query_to_relational(
        resolved_query,
        function,
        user_alias.clone(),
        &fold.core.identities,
    )?;
    // THE EXPANSION PUBLISHES THE AUTHORED HEADING. A fixpoint's support —
    // the caller actuals its frontier carried — is the caller's own
    // occurrence, which the caller already stands over. It crosses every
    // read inside the body and is dropped HERE, once, at the boundary.
    let resolved_expr = drop_frontier_actuals(resolved_expr, fold)?;

    // A plain scalar parameter whose name collides with a column the body
    // would otherwise resolve to silently captures it. Refuse loudly instead —
    // except where the caller SUPPLIED the parameter under the same
    // spelling, which is the author's own written binding.
    {
        use crate::pipeline::asts::ddl::HoColumnKind;
        let body_schema = resolved_expr.semantic_relation();
        let body_cols = crate::relation::published_ports(&fold.core.identities, &body_schema)?;
        for pos in &positions {
            if !matches!(pos.column_kind, HoColumnKind::Scalar) || pos.ground_pattern.is_some() {
                continue;
            }
            let Some(param_name) = pos.column_name.as_deref() else {
                continue;
            };
            let spelling = fold.core.identities.intern(param_name, false);
            let param = fold.core.identities.canonical(spelling);
            let supplied_under_the_same_name =
                authored_bare.get(param_name).is_some_and(|written| {
                    fold.core
                        .identities
                        .canonical(fold.core.identities.intern(written, false))
                        == param
                });
            if supplied_under_the_same_name {
                continue;
            }
            let collisions: Vec<String> = body_cols
                .iter()
                .filter_map(|column| {
                    if fold.core.identities.published_sym(column.column()) != Some(param) {
                        return None;
                    }
                    Some(format!("column '{param_name}' in the view body"))
                })
                .collect();
            if !collisions.is_empty() {
                return Err(DelightQLError::from(HoDefinition::ParamShadowsColumn {
    message: format!(
                        "Scalar parameter '{param}' of higher-order view '{func}' collides with \
                         {collisions}. The parameter would \
                         silently capture the column: body constraints on '{param}' tautologize and \
                         the column drops from the output. Rename the parameter (e.g. '{param}_arg') \
                         so it no longer shadows the column.",
                        param = param_name,
                        func = function,
                        collisions = collisions.join(", "),
                    ),
}));
            }
        }
    }

    // Apply the call-site scalar spec over the RESOLVED result. A call with
    // scalar parameters always carries a supplied scalar row; `All` therefore
    // has no discriminator positions to expose or hide.
    if matches!(scalar_spec, ast_unresolved::Access::All) {
        let expr =
            apply_ho_access_pattern(access_spec, resolved_expr, function, &user_alias, fold)?;
        return Ok(expr);
    }

    let body_schema = resolved_expr.semantic_relation();
    let scalar_exprs = match scalar_spec {
        ast_unresolved::Access::Slots(exprs) => exprs,
        _ => {
            let expr =
                apply_ho_access_pattern(access_spec, resolved_expr, function, &user_alias, fold)?;
            return Ok(expr);
        }
    };

    // Identify scalar column names from position analysis
    let scalar_positions: Vec<&crate::pipeline::asts::ddl::HoPositionInfo> = positions
        .iter()
        .filter(|p| {
            matches!(
                p.column_kind,
                crate::pipeline::asts::ddl::HoColumnKind::Scalar
            )
        })
        .collect();
    let scalar_col_names: Vec<Option<&str>> = scalar_positions
        .iter()
        .map(|p| p.column_name.as_deref())
        .collect();

    // Build WHERE constraints and column filtering for scalar positions
    // from the AUTHORED spec — literals compare, bare names witness, `_`
    // hides — over the resolved body's own published heading.
    let input_scope = body_schema;
    let schema_cols = crate::relation::published_ports(&fold.core.identities, &input_scope)?;
    let scalar_col_names: Vec<_> = scalar_col_names
        .into_iter()
        .map(|name| {
            name.map(|name| {
                let spelling = fold.core.identities.intern(name, false);
                fold.core.identities.canonical(spelling)
            })
        })
        .collect();

    // Match authored scalar binders by their published identity. A
    // pure-ground position is deliberately hygienic and therefore publishes
    // no spelling; construction places those injected discriminator columns
    // at the glob projection's tail (or before an explicit result head).
    // That boundary placement is exhaustive and lets this judgment retain
    // their exact ports without a generated-name convention.
    let mut scalar_columns: Vec<Option<crate::relation::PortId>> = scalar_col_names
        .iter()
        .map(|name| {
            name.and_then(|name| {
                schema_cols.iter().copied().find(|column| {
                    fold.core.identities.published_sym(column.column()) == Some(name)
                })
            })
        })
        .collect();
    let unmatched_ground: Vec<usize> = scalar_positions
        .iter()
        .enumerate()
        .filter(|(index, position)| {
            scalar_columns[*index].is_none()
                && position.ground_pattern
                    == Some(crate::pipeline::asts::ddl::HoGroundPattern::AllClauses)
        })
        .map(|(index, _)| index)
        .collect();
    if !unmatched_ground.is_empty() {
        let mut boundary: Vec<_> = if output_is_glob {
            schema_cols
                .iter()
                .copied()
                .rev()
                .filter(|column| {
                    fold.core.identities.addressing(column.column())
                        == crate::names::Addressing::Hygienic
                        && fold
                            .core
                            .identities
                            .authority()
                            .residual_row_token(*column)
                            .is_none()
                })
                .take(unmatched_ground.len())
                .collect()
        } else {
            schema_cols
                .iter()
                .copied()
                .filter(|column| {
                    fold.core.identities.addressing(column.column())
                        == crate::names::Addressing::Hygienic
                        && fold
                            .core
                            .identities
                            .authority()
                            .residual_row_token(*column)
                            .is_none()
                })
                .take(unmatched_ground.len())
                .collect()
        };
        if output_is_glob {
            boundary.reverse();
        }
        if boundary.len() != unmatched_ground.len() {
            return Err(Internal::invariant(
                "closed higher-order application",
                "a pure-ground higher-order position lost its injected discriminator column",
            ));
        }
        for (index, column) in unmatched_ground.into_iter().zip(boundary) {
            scalar_columns[index] = Some(column);
        }
    }
    let mut where_constraints = Vec::new();
    let mut slots = Vec::with_capacity(schema_cols.len());
    // A GROUND SCALAR POSITION PUBLISHES NOTHING; it is a DEPENDENCY of the
    // access, read by the discriminating predicate and by nothing else.
    let mut dependencies = Vec::new();
    let mut carried = Vec::with_capacity(schema_cols.len());

    for col in schema_cols.iter().copied() {
        let scalar_idx = scalar_columns
            .iter()
            .position(|column| *column == Some(col));
        let mut naming = crate::relation::form::Naming::Inherited;
        if let Some(scalar_idx) = scalar_idx.filter(|index| *index < scalar_exprs.len()) {
            let scalar_expr = scalar_exprs[scalar_idx].term();
            let position_param = scalar_positions
                .get(scalar_idx)
                .and_then(|position| position.column_name.as_deref());
            let Some(scalar_expr) = scalar_expr else {
                carried.push(col);
                slots.push(crate::relation::form::ProjectSlot::Carried {
                    source: col,
                    naming,
                });
                continue;
            };
            match scalar_expr {
                ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(value),
                ) => {
                    dependencies.push(col);
                    // A GROUND SCALAR ARGUMENT IS A MATCH ARM, not a join:
                    // the caller's own row is asked whether this cell holds
                    // that value, and one row cannot multiply against
                    // itself. The language's null-safe equality is what a
                    // `null` argument needs to select a `null` cell.
                    where_constraints.push(ast_resolved::TruthExpression::Comparison(Comparison {
                        operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
                        left: Box::new(ast_resolved::DomainExpression::Reference(
                            Reference::Named(NamedReference(ColumnOccurrence::engine(col))),
                        )),
                        right: Box::new(ast_resolved::DomainExpression::Application(
                            ast_resolved::FunctionApplication::Ground(value.clone()),
                        )),
                    }));
                    continue;
                }
                ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                    AuthoredColumn { name, .. },
                ))) => {
                    // THE CARRIER IS THE EXACT OCCURRENCE. The caller
                    // resolved this bare-name actual to a port of a carrier
                    // the body read; the position of the expansion's heading
                    // that CONTINUES that port — by the continuation edge
                    // every continuing act wrote since the read — is that
                    // occurrence: never a same-spelled sibling (another
                    // value), never a second position republishing the value
                    // (a copy is not the occurrence), while a renamed
                    // continuation is. The carrier standing beside itself
                    // (a self-join) refuses; an actual the caller's world
                    // could not answer has already refused before the body
                    // opened, so a missing carrier here cannot be an output.
                    let actual_port = position_param
                        .and_then(|param| spent_actuals.get(&SqlIdentifier::new(param)))
                        .and_then(|value| match value {
                            ast_resolved::DomainExpression::Reference(Reference::Named(
                                NamedReference(occurrence),
                            )) => Some(occurrence.column),
                            _ => None,
                        });
                    let carriers: Vec<_> = match actual_port {
                        Some(actual) => schema_cols
                            .iter()
                            .copied()
                            .filter(|candidate| {
                                *candidate != col
                                    && fold
                                        .core
                                        .identities
                                        .continues_occurrence(*candidate, actual)
                            })
                            .collect(),
                        None => Vec::new(),
                    };
                    let carrier = match carriers.as_slice() {
                        [] => None,
                        [carrier] => Some(*carrier),
                        several => {
                            return Err(DelightQLError::from(
                                HoDefinition::ActualAmbiguousOccurrence {
                                    message: format!(
                                    "the actual bound to '{name}' of '{function}' continues at \
                                     {} positions of the expansion's heading — the carrier \
                                     stands beside itself — and the dispatch witness \
                                     constrains one occurrence",
                                    several.len()
                                ),
                                },
                            ));
                        }
                    };
                    match carrier {
                        // The scalar slot is a dispatch witness for the caller
                        // lvar already carried in the body. It constrains that
                        // occurrence and publishes no second column.
                        Some(carrier) => {
                            dependencies.push(col);
                            // THE WITNESS CONSTRAINS ONE ROW'S TWO CELLS —
                            // the expansion's position and the carrier the
                            // caller already published there. Nothing
                            // multiplies, so this is the language's equality.
                            where_constraints.push(ast_resolved::TruthExpression::Comparison(
                                Comparison {
                                    operator:
                                        crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
                                    left: Box::new(ast_resolved::DomainExpression::Reference(
                                        Reference::Named(NamedReference(ColumnOccurrence::engine(
                                            col,
                                        ))),
                                    )),
                                    right: Box::new(ast_resolved::DomainExpression::Reference(
                                        Reference::Named(NamedReference(ColumnOccurrence::engine(
                                            carrier,
                                        ))),
                                    )),
                                },
                            ));
                            continue;
                        }
                        None if position_param
                            .is_some_and(|param| join_bound_scalars.contains(param)) =>
                        {
                            // Join-bound ground clauses already compare this
                            // exact caller row while each clause is shaped.
                            // The accumulated position is therefore only the
                            // private dispatch dependency, never an output.
                            dependencies.push(col);
                            continue;
                        }
                        None => return Err(incomplete_scalar_actual(function, name.as_str())),
                    }
                }
                ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Open(
                        ast_unresolved::DomainHole::Disregarded,
                    ),
                ) => {
                    naming = crate::relation::form::Naming::Hygienic;
                }
                _ => {
                    let actual = position_param
                        .and_then(|param| spent_actuals.get(&SqlIdentifier::new(param)))
                        .cloned()
                        .ok_or_else(|| {
                            incomplete_scalar_actual(
                                function,
                                position_param.unwrap_or("scalar parameter"),
                            )
                        })?;
                    dependencies.push(col);
                    where_constraints.push(ast_resolved::TruthExpression::Comparison(Comparison {
                        operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
                        left: Box::new(ast_resolved::DomainExpression::Reference(
                            Reference::Named(NamedReference(ColumnOccurrence::engine(col))),
                        )),
                        right: Box::new(actual),
                    }));
                    continue;
                }
            }
        }
        carried.push(col);
        slots.push(crate::relation::form::ProjectSlot::Carried {
            source: col,
            naming,
        });
    }
    // THE CALL SITE'S ACCESS. The published positions are the ones a caller
    // can name; the ground discriminators are dependencies its predicate
    // reads and its heading does not carry.
    let _ = &carried;
    let mut expr = resolved_expr.republished(|chain| {
        fold.core.identities.authority().extend(
            chain,
            crate::relation::builder::StepOp::Access {
                shape: crate::relation::form::AccessShape::Named,
                slots: &slots,
                dependencies: &dependencies,
            },
        )
    })?;
    let output_scope = expr.semantic_relation();

    // Wrap in Filter if there are WHERE constraints
    if !where_constraints.is_empty() {
        let combined = combine_where_constraints(where_constraints);
        expr = expr.transparently(ast_resolved::Transparent::Restrict {
            condition: combined,
            origin: ast_resolved::FilterOrigin::HoGroundScalar,
        });
    }

    // A discriminator dependency must remain readable by the restriction
    // beneath this point, but it is not part of the published heading. Force
    // one relational boundary after that read so SQL generation cannot carry
    // the hidden cell into a top-level result merely to service the filter.
    if !dependencies.is_empty() {
        let input = expr.semantic_relation();
        let published = crate::relation::published_ports(&fold.core.identities, &input)?;
        let projected: Vec<_> = published
            .iter()
            .copied()
            .map(|source| crate::relation::form::ProjectSlot::Carried {
                source,
                naming: crate::relation::form::Naming::Inherited,
            })
            .collect();
        expr = expr.republished(|chain| {
            fold.core.identities.authority().extend(
                chain,
                crate::relation::builder::StepOp::Republish {
                    of: crate::relation::builder::Republishing::Project(
                        crate::relation::form::ProjectSpec {
                            input,
                            slots: &projected,
                            why: crate::relation::form::ProjectWhy::Restate,
                        },
                    ),
                    sources: published,
                },
            )
        })?;
    }
    let _ = output_scope;
    // The transparent (no-CTE) HO path carries a call-site alias only in
    // the lexical state; rebuilding that state after applying scalar
    // arguments must not silently discard the alias.
    let resolved = expr;
    let resolved = match &user_alias {
        Some(alias) => {
            let spelling = fold
                .core
                .identities
                .intern(alias.as_str(), alias.is_stropped());
            resolved.aliased(spelling, &fold.core.identities)?
        }
        None => resolved,
    };
    let resolved = apply_ho_access_pattern(access_spec, resolved, function, &user_alias, fold)?;

    Ok(resolved)
}

/// Republish an expansion's heading without the fixpoint support it
/// carried: every position marked as a frontier actual is projected away,
/// the rest continue as they stand.
fn drop_frontier_actuals(
    expr: ResolvedRelation,
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold<'_, '_>,
) -> Result<ResolvedRelation> {
    let input = expr.semantic_relation();
    let published = crate::relation::published_ports(&fold.core.identities, &input)?;
    let authority = fold.core.identities.authority();
    let kept: Vec<_> = published
        .iter()
        .copied()
        .filter(|port| authority.frontier_actual(*port).is_none())
        .collect();
    if kept.len() == published.len() {
        return Ok(expr);
    }
    let slots: Vec<_> = kept
        .iter()
        .copied()
        .map(|source| crate::relation::form::ProjectSlot::Carried {
            source,
            naming: crate::relation::form::Naming::Inherited,
        })
        .collect();
    expr.republished(|chain| {
        fold.core.identities.authority().extend(
            chain,
            crate::relation::builder::StepOp::Republish {
                of: crate::relation::builder::Republishing::Project(
                    crate::relation::form::ProjectSpec {
                        input,
                        slots: &slots,
                        why: crate::relation::form::ProjectWhy::Restate,
                    },
                ),
                sources: kept,
            },
        )
    })
}

/// The trailing access group on a parameterized-rule call is ordinary
/// argumentative access over the declared heading — uniform with plain
/// rules and receipt access (ruling R-3), and CONSUMED THROUGH THE ONE
/// argumentative act: `ResolvedRelation::patterned` decides every slot,
/// records each binding's exact reuse against the live left interface,
/// and the caller-pattern bind writes the record. Hygienic
/// carriers (injected scalar discriminators, param labels) are split off
/// by that act and ride the read as dependencies. There is no
/// source-specific slot semantics here: only the SURFACE gate survives —
/// bare names, `_`, and literals bind on a parameterized-rule access
/// pattern for now, and anything wider refuses by name.
fn apply_ho_access_pattern(
    access_spec: &ast_unresolved::Access,
    expr: ResolvedRelation,
    function: &str,
    user_alias: &Option<SqlIdentifier>,
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold<'_, '_>,
) -> Result<ResolvedRelation> {
    let ast_unresolved::Access::Slots(slots) = access_spec else {
        // Glob access stays payload-transparent.
        return Ok(expr);
    };

    // THE SURFACE GATE, unchanged: which slot shapes a parameterized-rule
    // access admits is a ruled surface question, not slot semantics. It is
    // read off the authored slots alone.
    for (position, slot) in slots.iter().enumerate() {
        match slot {
            ast_unresolved::Slot::Bind(_) | ast_unresolved::Slot::Anon => {}
            ast_unresolved::Slot::Constraint(term)
                if matches!(
                    &**term,
                    ast_unresolved::DomainExpression::Application(
                        ast_unresolved::FunctionApplication::Ground(_)
                    )
                ) => {}
            other => {
                return Err(DelightQLError::from(Resolution::HoAccessPatternShape {
                    message: format!(
                        "'{function}' access pattern position {}: \
                         only bare names, `_`, and literals bind on a parameterized-rule \
                         access pattern for now. Got: {other:?}. Bind the column to a name \
                         and constrain it with an explicit predicate instead.",
                        position + 1,
                    ),
                }));
            }
        }
    }

    let owner = match user_alias {
        Some(alias) => crate::pipeline::resolver::PatternOwner::Authored(alias.clone()),
        None => crate::pipeline::resolver::PatternOwner::Unqualified,
    };
    ResolvedRelation::patterned(
        crate::pipeline::resolver::PatternOperand::Standing(expr),
        access_spec,
        owner,
        fold,
    )?
    .restricted_by_its_own_constraints(&fold.core.identities)
}

/// Inject ground scalar constants as real AST columns into a clause body.
///
/// For each position where this clause has GroundScalar, wraps the body's
/// main query expression with a General (embed) operator:
///   `body |> (*, "ground_value" as column_name)`
///
/// Column names come from cross-clause position analysis: a position grounded
/// in only some clauses uses the free-clause name, while one grounded in every
/// clause uses the declared parameter name.
///
/// When only some clauses ground the position, a free clause must export it
/// column too, carrying the CALLER's literal (its own substituted
/// value) — otherwise the union pads the column NULL and the
/// call-site filter (`x = 'a' AND y = 'c'`) kills every clause: the
/// whole entity silently empties. A caller lvar is injected too: the
/// caller-owned carrier is already in the clause body, so the discriminator
/// can publish the row value that selected this clause.
///
/// If `output_head` is Some, also applies the argumentative output projection.
pub(in crate::defuse) fn inject_scalar_columns(
    query: ast_unresolved::Query,
    clause_params: &[HoParam],
    positions: &[HoPositionInfo],
    output_head: Option<&[HeadItem]>,
    actuals: &super::bound_use::HoActuals,
    carry_caller_lvars: bool,
) -> ast_unresolved::Query {
    use crate::pipeline::asts::core::PipeOp;

    // THE DISCRIMINATORS GO OUT IN POSITION ORDER. Clause accumulation is
    // positional, so slot k of every clause has to be position k's
    // discriminator; emitting the grounds and then the frees puts a ground
    // clause's position-0 column where a mixed clause's position-1 column
    // stands, and the union then answers each call's filter with the other
    // position's value. One ordered list, indexed by the position it
    // discriminates.
    let mut injections: Vec<(usize, String, ast_unresolved::DomainExpression)> = Vec::new();
    for pos_info in positions {
        if let Some(clause_param) = clause_params.get(pos_info.position) {
            // A glob already republishes an identically named caller lvar.
            // Ground clauses constrain that occurrence before UNION; free
            // clauses bind it directly. Neither needs a second scalar column.
            let glob_already_carries_position = output_head.is_none()
                && carry_caller_lvars
                && pos_info.column_name.as_ref().is_some_and(|column_name| {
                    actuals
                        .authored_bare
                        .get(column_name.as_str())
                        .is_some_and(|written| written == column_name.as_str())
                });
            match clause_param {
                HoParam::Ground { text, .. } => {
                    if !glob_already_carries_position {
                        if let Some(name) =
                            pos_info.column_name.as_ref().map(|name| name.to_string())
                        {
                            let literal =
                                crate::pipeline::asts::core::LiteralValue::from_stored_ground(text);
                            injections.push((
                                pos_info.position,
                                name,
                                ast_unresolved::DomainExpression::Application(
                                    ast_unresolved::FunctionApplication::Ground(literal),
                                ),
                            ));
                        }
                    }
                }
                HoParam::Scalar {
                    name: param_name, ..
                } if pos_info.ground_pattern
                    == Some(crate::pipeline::asts::ddl::HoGroundPattern::SomeClauses) =>
                {
                    // EVERY CLAUSE EMITS THE POSITION, OR NONE DOES. A
                    // ground clause emits its literal here; a free clause
                    // that emitted nothing would make the accumulation
                    // ragged by the compiler's own hand, and the two
                    // headings would then line up at whatever width the
                    // bodies happen to share — silently pairing a
                    // discriminator with a body column. The one condition
                    // is the shared one: where a glob already republishes
                    // the position, neither branch emits.
                    if !glob_already_carries_position {
                        let supplied =
                            actuals
                                .values
                                .contains_key(&delightql_types::SqlIdentifier::new(
                                    param_name.as_str(),
                                ));
                        if let (Some(name), true) = (
                            pos_info.column_name.as_ref().map(|name| name.to_string()),
                            supplied,
                        ) {
                            // THE RECORDED OCCURRENCE (ruling
                            // 2026-08-26): the body resolves this formal
                            // through its frame — the exact value the
                            // caller resolved — and the splice re-anchors
                            // to the body-port occurrence the carrier's
                            // OWN construction record names. Never a
                            // spelling, ordinal, or resemblance; a
                            // same-spelled sibling cannot capture it.

                            let formal = ast_unresolved::DomainExpression::Reference(
                                Reference::Named(NamedReference(AuthoredColumn {
                                    name: param_name.clone(),
                                    qualifier: None,
                                    namespace_path: ast_unresolved::NamespacePath::empty(),
                                })),
                            );
                            injections.push((pos_info.position, name, formal));
                        }
                    }
                }
                _ => {}
            }
        }
    }

    if injections.is_empty() && output_head.is_none() {
        return query;
    }
    injections.sort_by_key(|(position, _, _)| *position);

    // Build the embed expressions
    let mut embed_items: Vec<ast_unresolved::OutItem> = Vec::new();
    let named = |expr: ast_unresolved::DomainExpression, name: &String| {
        ast_unresolved::OutItem::One(ast_unresolved::OneOut::authored(
            expr,
            Some(name.clone().into()),
        ))
    };

    if output_head.is_some() {
        // When there's an output head, the discriminators are part of the
        // projection, ahead of the head's own items.
        for (_, col_name, expression) in &injections {
            embed_items.push(named(expression.clone(), col_name));
        }
        // Then: output head items
        if let Some(items) = output_head {
            for item in items {
                // NOTE: HO output-head positions do NOT yet honor `as`-labels. The
                // label parses (view_head_item is shared with rule heads) and is carried
                // in the AST, but is ignored here — a labeled HO output item is refused
                // earlier, at DDL build time (`ddl/head/ho_label_unsupported`), so this
                // code never sees one. Head-`as` on HO output positions is future work.
                match &item.supply {
                    crate::pipeline::asts::ddl::Supply::Ref(name) => {
                        embed_items.push(ast_unresolved::OutItem::one(
                            ast_unresolved::OneOut::authored(
                                ast_unresolved::DomainExpression::lvar_builder(name.clone())
                                    .build(),
                                None,
                            ),
                        ));
                    }
                    crate::pipeline::asts::ddl::Supply::Ground(value) => {
                        embed_items.push(ast_unresolved::OutItem::one(
                            ast_unresolved::OneOut::authored(
                                ast_unresolved::DomainExpression::Application(
                                    ast_unresolved::FunctionApplication::Ground(value.clone()),
                                ),
                                None,
                            ),
                        ));
                    }
                }
            }
        }
    } else {
        // No output head (glob) — use embed: (*, "value" as name, ...)
        embed_items.push(ast_unresolved::OutItem::Many(
            crate::pipeline::asts::core::Spread::Glob(crate::pipeline::asts::core::Glob::whole()),
        ));
        for (_, col_name, expression) in &injections {
            embed_items.push(named(expression.clone(), col_name));
        }
    }

    let operator = PipeOp::Project(
        crate::pipeline::asts::vocabulary::Vec1::try_from_vec(embed_items)
            .expect("the embed carries the glob it was built around"),
    );

    // Wrap the main query expression with the pipe operator
    wrap_query_with_pipe(query, operator)
}

/// Parse a literal value string (e.g., `"young"`, `42`, `::fast`, or
/// `` :`people(*)` ``) into a LiteralValue. Mention ground values
/// arrive already canonical (the DDL extractor canonicalizes at
/// consult time), so the wrapper is stripped, never re-parsed.
/// A provable miss is an error, not an empty relation: a knowable ground
/// argument — a literal written at the call site — at a position every clause grounds,
/// matching no clause head, is emptiness by absent
/// DECLARATION. The catalog proves it, so refuse with the declared
/// spellings instead of emitting a provably-empty query. A free
/// clause at the position makes every call satisfiable
/// — no refusal can fire there; a data-borne argument (lvar,
/// expression) keeps relational semantics and misses to empty.
pub(in crate::defuse) fn refuse_provable_ground_miss(
    function: &str,
    scalar_spec: &ast_unresolved::Access,
    positions: &[HoPositionInfo],
) -> Result<()> {
    use crate::pipeline::asts::ddl::{HoColumnKind, HoGroundPattern};

    let ast_unresolved::Access::Slots(exprs) = scalar_spec else {
        return Ok(());
    };
    let scalar_positions: Vec<&HoPositionInfo> = positions
        .iter()
        .filter(|p| matches!(p.column_kind, HoColumnKind::Scalar))
        .collect();
    for (idx, pos) in scalar_positions.iter().enumerate() {
        if pos.ground_pattern != Some(HoGroundPattern::AllClauses) {
            continue;
        }
        let Some(value) = exprs.get(idx).and_then(ast_unresolved::Slot::ground) else {
            continue;
        };
        let any_match = pos.ground_values.iter().any(|(_, clause_val)| {
            ground_literals_equal(
                &crate::pipeline::asts::core::LiteralValue::from_stored_ground(clause_val),
                value,
            )
        });
        if !any_match {
            let mut spellings: Vec<&str> =
                pos.ground_values.iter().map(|(_, s)| s.as_str()).collect();
            spellings.dedup();
            return Err(DelightQLError::from(Grounding::HeadProvableMiss {
                message: format!(
                    "no clause of '{function}' grounds on '{arg}' at parameter {n} — \
                     emptiness by absent declaration is an error, not a result. \
                     Declared spellings: {list}. A data-borne value (a column, not \
                     a literal) misses to empty instead.",
                    arg = value.stored_ground(),
                    n = pos.position + 1,
                    list = spellings.join(", "),
                ),
            }));
        }
    }
    Ok(())
}

/// Equality for ground-head selection at compile time. Same-variant
/// byte equality; numbers compare by EXACT decimal value (the SQL
/// comparison the selection lowers to treats 5 and 5.0 as equal, and
/// distinguishes adjacent integers above 2^53 that any f64 road would
/// merge — a merged pair here passes the provable-miss check and then
/// misses in SQL, the silent empty the law forbids); differing
/// variants never match (an untyped injected column compares TEXT vs
/// INTEGER by type ordering, never equal).
/// A relation landed at a formal that cannot receive one.
///
/// THE POSITION SAYS WHICH FORMAL, and the position is all that is known: the
/// default landing and an authored `@` name a formal the same way, so one
/// refusal serves both and neither is told apart by the glyph the author
/// used. The FINAL position teaches toward `@`, because moving the landing is
/// the remedy there; an earlier one was reached by an `@` already and teaches
/// toward the table parameter it should have named.
fn incomplete_scalar_actual(entity: &str, parameter: &str) -> DelightQLError {
    DelightQLError::from(Ho::IncompleteApplication {
    message: format!(
            "parameter '{parameter}' of '{entity}' has no exact caller value — every member of a parameter row is required input before the body opens"
        ),
})
}

fn ground_literals_equal(
    a: &crate::pipeline::asts::core::LiteralValue,
    b: &crate::pipeline::asts::core::LiteralValue,
) -> bool {
    use crate::pipeline::asts::core::LiteralValue::*;
    match (a, b) {
        (Number(x), Number(y)) => match (normalize_number(x), normalize_number(y)) {
            (Some(p), Some(q)) => p == q,
            _ => x == y,
        },
        _ => a == b,
    }
}

/// Exact decimal value of a numeric spelling, as (sign, significant
/// digits, exponent) with value = sign × 0.<digits> × 10^exp; zero is
/// (0, "", 0). Arbitrary precision — no float on the road — so every
/// distinct value normalizes distinctly and every equal value ("12",
/// "12.0", "1.2e1") normalizes identically. None for spellings that
/// are not plain decimal/exponent numbers.
fn normalize_number(s: &str) -> Option<(i8, String, i64)> {
    let s = s.trim();
    let (sign, rest) = match s.as_bytes().first()? {
        b'-' => (-1i8, &s[1..]),
        b'+' => (1, &s[1..]),
        _ => (1, s),
    };
    let (mantissa, exp10) = match rest.find(['e', 'E']) {
        Some(i) => (&rest[..i], rest[i + 1..].parse::<i64>().ok()?),
        None => (rest, 0),
    };
    let (int_part, frac_part) = match mantissa.find('.') {
        Some(i) => (&mantissa[..i], &mantissa[i + 1..]),
        None => (mantissa, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let digits: String = int_part.chars().chain(frac_part.chars()).collect();
    let mut exp = int_part.len() as i64 + exp10;
    let stripped = digits.trim_start_matches('0');
    exp -= (digits.len() - stripped.len()) as i64;
    let stripped = stripped.trim_end_matches('0');
    if stripped.is_empty() {
        return Some((0, String::new(), 0));
    }
    Some((sign, stripped.to_string(), exp))
}

/// Wrap a Query's main expression with a pipe operator.
///
/// Wrap the query's BODY with a pipe operator; the bindings ride along.
fn wrap_query_with_pipe(
    mut query: ast_unresolved::Query,
    operator: ast_unresolved::PipeOp,
) -> ast_unresolved::Query {
    query.body = query
        .body
        .then(Step::authored(ast_unresolved::Continuation::Pipe {
            operator,
            named: None,
        }));
    query
}

/// THE ROW ARRIVES WHOLE, landed member included, and is ADMITTED against
/// the selected declaration before any formal is bound. The pairing is the
/// authority's ([`Application::admit`]): the landed member faces the formal
/// at the place the build put it, a complete left prefix stands beside it,
/// and every member has a formal — so this position never pairs a source
/// with a formal it was not put at, and never has to decide what a source
/// with no position, or a position with no source, would mean.
///
/// What happens here is the PURE spend of each pair: a carrier for a
/// relation formal, a caller-resolved value for a scalar one, a residual
/// construction for a rule one, a dispatch term for a ground one.
fn split_ho_first_parens(
    entity: &HoSelection,
    arguments: &ast_unresolved::CallArguments,
    start_at: usize,
    // Ordinary applications and residual spends complete the declared
    // suffix. Residual construction completes only its validated left-prefix
    // frontier; the structural residual signature owns what remains, and no
    // body opens until a later spend supplies that suffix.
    completion: ParamRowCompletion,
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold<'_, '_>,
    mut inherited_evaluation_row: super::carriers::ResidualEvaluationRow<'_>,
) -> Result<(
    HoParamBindings,
    Vec<(String, ast_unresolved::DomainExpression)>,
    std::collections::HashMap<SqlIdentifier, RuleValueId>,
    ast_unresolved::Access,
    Option<crate::defuse::carriers::CarrierRecord>,
    usize,
)> {
    use crate::pipeline::asts::ddl::HoParam as RegParam;

    // Position analysis records which supplied scalars need clause dispatch — a family fact
    // (catalog-stored positions, or head analysis; a stored text that
    // does not reconstruct is catalog corruption, never a default).
    let positions = entity.positions()?;
    let entity_name = entity.name();

    // THE ADMISSION. The first parens are arguments, not dimensions: the
    // members this function spends are the ho_arguments themselves, each
    // already standing at the one formal its position names.
    let application = Application::admit(
        &entity_name,
        entity.params(),
        arguments,
        start_at,
        completion,
    )?;

    let mut bindings = HoParamBindings::default();
    let mut scalar_actuals: Vec<(String, ast_unresolved::DomainExpression)> = Vec::new();
    let mut rule_actuals = std::collections::HashMap::new();
    let mut pending_rule_actuals = Vec::new();
    let mut scalar_exprs = Vec::new();

    for pair in application.pairs() {
        let param_idx = pair.position();
        let param = pair.formal();
        let actual = pair.actual();

        // THE LANDED PAIR WAS SPENT ABOVE, on its own carrier; the admission
        // already held it to a relation formal.
        if actual.is_landed() {
            continue;
        }

        match param {
            // THE RELATION PAIRS WERE SPENT ABOVE, by the carrier authority.
            RegParam::Relation { .. } => continue,
            RegParam::Scalar { .. } => {
                // A position grounded in only some clauses needs both text
                // substitution and the slot row's own constraint.
                let is_mixed_ground = positions.iter().any(|pi| {
                    pi.position == param_idx
                        && pi.ground_pattern
                            == Some(crate::pipeline::asts::ddl::HoGroundPattern::SomeClauses)
                });

                // THE DESCRIPTOR DECIDES WHAT A LIFTED GROUP IS. `&` bounds
                // arguments and dissolves into an anonymous relation, so a
                // scalar written after it arrives here as a one-row, one-column
                // relation. The formal says it is a scalar, and that is the
                // set-at-a-time reading: the row IS the value. Left as the
                // relation, the parameter would carry a placeholder spelling
                // into the body and refuse there under a name nobody wrote.
                let expr = match actual.lifted_scalar() {
                    Some(value) => value,
                    None => require_term(actual, param, &entity_name, param_idx)?,
                };

                // THE FORMAL IS DECLARED, THE ACTUAL IS COLLECTED: the
                // normalizer keeps the name a reference (a slot with it
                // constrains), and the caller resolves the actual into the
                // body's frame before admission. A literal actual is also a
                // VALUE fact, for the one pre-resolution consumer (a bound).
                bindings.scalar_formals.insert(param.name().to_string());
                if let ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(value),
                ) = &expr
                {
                    bindings
                        .scalar_literals
                        .insert(param.name().to_string(), value.clone());
                }
                scalar_actuals.push((param.name().to_string(), expr.clone()));

                if is_mixed_ground {
                    // Preserve the supplied value for clause dispatch.
                    scalar_exprs.push(expr);
                }
            }
            RegParam::Rule { signature, .. } => {
                let Some(designator) = actual.designator() else {
                    return Err(DelightQLError::from(Ho::RuleValueForm {
                        message: format!(
                            "parameter '{}' of '{}' requires a closed residual rule value",
                            param.name(),
                            entity_name
                        ),
                    }));
                };
                pending_rule_actuals.push((
                    param.name().clone(),
                    signature.clone(),
                    designator.clone(),
                ));
            }
            RegParam::Ground { .. } => {
                // A ground position reaches the slot row via scalar_exprs
                let supplied = require_term(actual, param, &entity_name, param_idx)?;
                scalar_actuals.push((param.name().to_string(), supplied.clone()));
                if let ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(value),
                ) = &supplied
                {
                    if let Some(name) = positions
                        .iter()
                        .find(|position| position.position == param_idx)
                        .and_then(|position| position.column_name.as_ref())
                    {
                        bindings
                            .scalar_literals
                            .insert(name.to_string(), value.clone());
                    }
                }
                scalar_exprs.push(supplied);
            }
        }
    }

    // THE RELATION FORMALS ARE BOUND by the carrier authority, from the
    // admitted row, once every pair's syntax has been judged in declared
    // order above: the landed source in the caller's run, every authored
    // actual closed, each under the face its formal declares. A configured
    // rule actual below may name a column of the landed relation; that
    // column is caller data and resolves over the record bound here. The
    // record is returned to expansion and reused, so nothing resolves a
    // flowing relation twice.
    let carriers = super::carriers::bind_relation_formals(
        fold,
        &entity_name,
        entity.params(),
        &application,
        &mut bindings,
    )?;
    // THE LANDED ROW, REALIZED: the exact landed row the enclosing
    // relational consumer would otherwise bind separately, as the record
    // holds it; a capture that augments it replaces that landing for the
    // whole consumer.
    let prepared_context = match bindings.formals.landed_source() {
        Some(landing) => Some(carriers.realized_formal(landing).ok_or_else(|| {
            Internal::invariant(
                "higher-order pipe landing",
                "the record binding the landed formal returns its realized row",
            )
        })?),
        None => None,
    };
    let mut prepared_pipe = (!carriers.is_empty()).then_some(carriers);

    // A LANDED ROW STANDS AT THE DESIGNATOR'S POSITION. Where the pipe
    // landed into this very call, that carrier is the construction row and
    // the enclosing join's own left member is not: the borrow simply ends
    // here, leaving that row standing for ordinary join assembly.
    let mut evaluation_row = match &prepared_context {
        Some(realized) => super::carriers::ResidualEvaluationRow::Realized(realized.clone()),
        None => inherited_evaluation_row.reborrow(),
    };

    for (formal, signature, designator) in pending_rule_actuals {
        let mut value =
            construct_residual(&designator, &signature, fold, evaluation_row.reborrow())?;
        let (crossed, captured) = fold.core.residuals.cross_into(
            value,
            &mut bindings,
            &mut prepared_pipe,
            &fold.core.identities,
        )?;
        value = crossed;
        if let Some(realized) = captured {
            evaluation_row = super::carriers::ResidualEvaluationRow::Realized(realized);
        }
        rule_actuals.insert(formal, value);
    }

    let scalar_spec = match Vec1::try_from_vec(
        scalar_exprs
            .into_iter()
            .map(ast_unresolved::Slot::classify)
            .collect(),
    ) {
        Some(slots) => ast_unresolved::Access::Slots(slots),
        None => ast_unresolved::Access::All,
    };

    Ok((
        bindings,
        scalar_actuals,
        rule_actuals,
        scalar_spec,
        prepared_pipe,
        application.supplied_through(),
    ))
}

/// The term a formal needs, or the refusal for the actual that has none.
///
/// A relation argument that is not a name — an anonymous table, an inner
/// relation, a call — has no term, and a skip mark computes nothing. The
/// formals that read a NAME or a VALUE say so here rather than reading an
/// invented lvar, which would refuse downstream under a spelling nobody
/// wrote or, worse, capture a real column that happened to share it.
fn require_term(
    actual: &Actual<'_>,
    param: &crate::pipeline::asts::ddl::HoParam,
    entity: &str,
    position: usize,
) -> Result<ast_unresolved::DomainExpression> {
    actual.term().ok_or_else(|| {
        DelightQLError::from(Ho::RelationalArgument {
            message: format!(
                "parameter '{}' of '{entity}' is supplied at position {position} by a \
                 relation expression, which names nothing this position can bind",
                param.name()
            ),
        })
    })
}

/// Ensure all HO position infos have column names.
/// For scalar positions, use the binder named by the assembled head. A
/// pure-ground position has no authored binder, but its `HoParam::Ground`
/// carries the construction-minted name shared with the injected column;
/// retaining that identity lets the finishing access hide the discriminator
/// without guessing by spelling, ordinal, or hygienic status.
pub(crate) fn ensure_position_column_names(
    positions: Vec<HoPositionInfo>,
    clauses: &[Clause],
) -> Vec<HoPositionInfo> {
    positions
        .into_iter()
        .map(|mut pi| {
            if pi.column_name.is_none() {
                for clause in clauses {
                    match clause.params().get(pi.position) {
                        Some(HoParam::Scalar { name, .. } | HoParam::Ground { name, .. }) => {
                            pi.column_name = Some(name.clone());
                            break;
                        }
                        _ => {}
                    }
                }
            }
            pi
        })
        .collect()
}

/// SQUISH ONE CLAUSE INTO THE BLOCK BEING BUILT: the clause query's own
/// query-local block is ABSORBED whole — claims and manifestations in the
/// same act, so the authored declaration order the clause was written
/// under travels with the bindings it governs — and its body becomes one
/// more claimless generated (or frontier) carrier under the function's
/// spelling.
pub(in crate::defuse) fn extract_clause_ctes(
    clause_query: ast_unresolved::Query,
    function: &str,
    frontier: Option<&super::instance::DefinitionFrontier>,
    caller: super::ClauseCaller,
    block: &mut crate::pipeline::asts::core::QueryLocalBlock,
) -> Result<()> {
    let ast_unresolved::Query { locals, body } = clause_query;
    block.absorb(locals)?;
    let authority = crate::pipeline::asts::core::CteAuthority {
        horizon: crate::pipeline::asts::core::LexicalHorizon::all(),
        head: crate::pipeline::asts::core::definitions::Head::glob(),
        origin: crate::pipeline::asts::core::provenance::CteOrigin::CompilerGenerated,
        // A parameterized head has no badge position.
        fixpoint: crate::pipeline::asts::vocabulary::Fixpoint::Bag,
    };
    // With an established fixpoint the clause binds UNDER THE FRONTIER, so
    // a later clause's same-key self-reference reads it by identity.
    block.admit_relation(match frontier {
        Some(frontier) => ast_unresolved::CteBinding::frontier(super::FrontierCte::new(
            body,
            frontier.clone(),
            authority,
            caller,
        )),
        None => ast_unresolved::CteBinding::authored(
            body,
            crate::pipeline::asts::core::AuthoredCteSubject::Generated {
                name: delightql_types::SqlIdentifier::new(function),
            },
            authority,
        ),
    })
}

/// Inject a cross-join with the input table into a clause body's FROM clause.
/// When an invocation supplies a caller lvar, every clause receives the caller
/// row: free heads bind it and ground heads filter it before clauses merge.
///
/// Wraps the body with a direct read of the caller input occurrence.
pub(in crate::defuse) fn inject_input_table_into_query(
    clause_query: ast_unresolved::Query,
    input_scope: crate::relation::StructuralRelation,
    input_condition: Option<ast_unresolved::TruthExpression>,
) -> ast_unresolved::Query {
    let input_table = ast_unresolved::Chain::read(
        ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Structural {
                pending: input_scope,
                authored_name: None,
                alias: None,
            },
            outer: false,
        },
        ast_unresolved::Access::All,
    );
    let input_table = if let Some(condition) = input_condition {
        input_table.then(Step::authored(ast_unresolved::Continuation::Restrict {
            condition: condition,
            origin: crate::pipeline::asts::core::FilterOrigin::HoGroundScalar,
        }))
    } else {
        input_table
    };

    let mut clause_query = clause_query;
    let (head, mut steps) = clause_query.body.into_parts();
    let barrier = steps
        .iter()
        .position(|step| matches!(step.form(), ast_unresolved::Continuation::Pipe { .. }))
        .unwrap_or(steps.len());
    let trailing = steps.split_off(barrier);
    let mut operand = ast_unresolved::Chain::ground(head);
    operand.continuations_mut().extend(steps);
    let mut body = input_table.then(Step::authored(ast_unresolved::Continuation::Member {
        rhs: operand,
        correlation: None,
        join_type: Some(crate::pipeline::asts::core::JoinType::Inner),
    }));
    body.continuations_mut().extend(trailing);
    clause_query.body = body;
    clause_query
}

/// A FIXPOINT CLAUSE CARRIES ITS CALLER THROUGH ITS OWN STAGES. Every
/// authored `|> (...)` projection inside a clause of a parameterized
/// fixpoint publishes the clause's carried formals ahead of its items, so
/// the caller's actual reaches the clause's last projection, where the
/// recursion authority publishes it beside the head as hidden support. A
/// formal is SPENT, never looked up: each stage's spelling of it lands on
/// the one position of the stage's row that continues the actual — the
/// admitted carrier's column in an anchor, the frontier's support or the
/// previous stage's own publication in a recursive clause — and a
/// spread-bearing stage already carries that position, so it takes none.
/// The head projection is added after this act and publishes the head
/// alone.
pub(in crate::defuse) fn carry_formals_through_stages(
    mut clause_query: ast_unresolved::Query,
    formals: &[delightql_types::SqlIdentifier],
) -> ast_unresolved::Query {
    use crate::pipeline::asts::core::PipeOp;
    if formals.is_empty() {
        return clause_query;
    }
    for step in clause_query.body.continuations_mut() {
        let ast_unresolved::Continuation::Pipe {
            operator: PipeOp::Project(items),
            ..
        } = step.form_mut()
        else {
            continue;
        };
        if items
            .iter()
            .any(|item| !matches!(item, ast_unresolved::OutItem::One(_)))
        {
            continue;
        }
        // An item that IS the bare formal already continues the actual,
        // under whatever name the author gave it; a second position would
        // be a copy standing beside the occurrence.
        let spelled_already = |formal: &delightql_types::SqlIdentifier| {
            items.iter().any(|item| {
                matches!(
                    item.value(),
                    Some(ast_unresolved::DomainExpression::Reference(Reference::Named(
                        NamedReference(AuthoredColumn {
                            name,
                            qualifier: None,
                            namespace_path,
                        }),
                    ))) if name == formal && namespace_path.is_empty()
                )
            })
        };
        let mut carried: Vec<ast_unresolved::OutItem> = formals
            .iter()
            .filter(|formal| !spelled_already(formal))
            .map(|formal| {
                ast_unresolved::OutItem::One(ast_unresolved::OneOut::authored(
                    ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                        AuthoredColumn {
                            name: formal.clone(),
                            qualifier: None,
                            namespace_path: ast_unresolved::NamespacePath::empty(),
                        },
                    ))),
                    Some(formal.clone()),
                ))
            })
            .collect();
        carried.extend(items.iter().cloned());
        *items =
            Vec1::try_from_vec(carried).expect("a projection with its carried formals is nonempty");
    }
    clause_query
}

/// A RECURSIVE CLAUSE'S GROUND-HEAD MATCH ARM. The clause admits no caller
/// carrier — its formals land on the frontier's carried actuals — so the
/// discriminator is a restriction over the clause's own row, placed where
/// the admitted road places the carrier's: before the first pipe barrier,
/// over every member the clause joins.
pub(in crate::defuse) fn restrict_before_barrier(
    clause_query: ast_unresolved::Query,
    condition: ast_unresolved::TruthExpression,
) -> ast_unresolved::Query {
    let mut clause_query = clause_query;
    let (head, mut steps) = clause_query.body.into_parts();
    let barrier = steps
        .iter()
        .position(|step| matches!(step.form(), ast_unresolved::Continuation::Pipe { .. }))
        .unwrap_or(steps.len());
    steps.insert(
        barrier,
        Step::authored(ast_unresolved::Continuation::Restrict {
            condition,
            origin: crate::pipeline::asts::core::FilterOrigin::HoGroundScalar,
        }),
    );
    let mut body = ast_unresolved::Chain::ground(head);
    body.continuations_mut().extend(steps);
    clause_query.body = body;
    clause_query
}

/// Build the constraint for a ground clause head against a caller lvar.
///
/// After UNION, a mixed position has one column identity even though one arm
/// bound the caller value and another arm supplied a ground discriminator.
/// Applying this predicate per arm preserves that distinction.
pub(in crate::defuse) fn ground_scalar_correlation_condition(
    clause_params: &[HoParam],
    positions: &[HoPositionInfo],
    actuals: &super::bound_use::HoActuals,
) -> Option<ast_unresolved::TruthExpression> {
    let conditions: Vec<_> = positions
        .iter()
        .filter_map(|position| {
            let clause_param = clause_params.get(position.position)?;
            let HoParam::Ground { text: value, .. } = clause_param else {
                return None;
            };
            let column_name = position.column_name.as_ref()?;
            // Only a BARE-NAME caller actual carries the row value the
            // discriminator compares against; the judgment is an authored
            // fact recorded in the caller.
            if !actuals.authored_bare.contains_key(column_name.as_str()) {
                return None;
            }
            // The left side is the FORMAL: the body's frame answers it with
            // the value the caller resolved — the carrier's own column.
            let formal = ast_unresolved::DomainExpression::Reference(Reference::Named(
                NamedReference(AuthoredColumn {
                    name: column_name.as_str().into(),
                    qualifier: None,
                    namespace_path: ast_unresolved::NamespacePath::empty(),
                }),
            ));
            // THE GROUND HEAD IS A MATCH ARM. The caller's row value is
            // tested against the clause's declared ground value, per row,
            // before the clauses accumulate — so a `null` actual selects the
            // `null` ground head. The target's equality answers UNKNOWN
            // there and empties the clause instead.
            Some(ast_unresolved::TruthExpression::Comparison(Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
                left: Box::new(formal),
                right: Box::new(ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(
                        crate::pipeline::asts::core::LiteralValue::from_stored_ground(value),
                    ),
                )),
            }))
        })
        .collect();
    ast_unresolved::TruthExpression::all(conditions)
}

/// How a higher-order invocation names its definition: through an
/// authored qualifier, or over the enlisted candidate set — where the
/// query's own common higher-order expressions are probed FIRST, nearest
/// wins, exactly as a bare callable probes the query's CFEs first.
pub(crate) enum HoNaming<'a> {
    Qualified(&'a [String]),
    Enlisted,
}

/// WHAT A PARAMETERIZED USE SELECTED: the query's own CHOE, or a consulted
/// family. Two environments, ONE road: everything after selection — the
/// landing, the carriers, the actual frame, the call-site spec, the access
/// — reads the selection through this one signature view.
#[derive(Debug, Clone)]
pub(in crate::defuse) enum HoSelection {
    Scoped(super::environment::ScopedHo),
    Family(super::admitted::ClosedHoFamily),
}

/// A residual's selected family and opening authority. A scoped definition
/// cannot inhabit this carrier without the lexical world captured by the
/// same construction; a consulted family already owns its declaration world
/// through `LinkedFamily`.
#[derive(Clone)]
enum ClosedSelection {
    Scoped {
        definition: super::environment::ScopedHo,
        world: super::environment::ClosedLexicalWorld,
    },
    Family(super::admitted::ClosedHoFamily),
}

impl ClosedSelection {
    fn name(&self) -> String {
        match self {
            ClosedSelection::Scoped { definition, .. } => definition.name().to_string(),
            ClosedSelection::Family(family) => family.name().to_string(),
        }
    }

    fn open(self) -> (HoSelection, Option<super::environment::ClosedLexicalWorld>) {
        match self {
            ClosedSelection::Scoped { definition, world } => {
                (HoSelection::Scoped(definition), Some(world))
            }
            ClosedSelection::Family(family) => (HoSelection::Family(family), None),
        }
    }
}

/// Opaque identity of one closed residual owned by the statement's existing
/// definition-use authority. Formal frames carry only this identity; the
/// selected family, declaration world, prefix and cursor cannot be paired
/// independently at a crossing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RuleValueId(pub(in crate::defuse) u32);

/// One constructed residual. Fields stay private to this module so the only
/// way to finish one is the owned spending road below.
#[derive(Clone)]
pub(crate) struct ClosedResidual {
    selection: ClosedSelection,
    declared: Vec<HoParam>,
    cursor: usize,
    signature: crate::pipeline::asts::core::definitions::ResidualSignature,
    pub(in crate::defuse) prefix: super::bound_use::HoActuals,
    prefix_scalar_spec: ast_unresolved::Access,
    capture: Option<super::carriers::ResidualCapture>,
}

/// Compilation-local storage for closed residuals. This is state of the one
/// definition-use lifecycle, not a name-indexed registry: insertion returns
/// an opaque identity and lookup can only retrieve the exact constructed
/// value.
#[derive(Default)]
pub(crate) struct ResidualStore {
    values: std::cell::RefCell<Vec<ClosedResidual>>,
}

impl ResidualStore {
    pub(crate) fn insert(&self, value: ClosedResidual) -> RuleValueId {
        let mut values = self.values.borrow_mut();
        let id = RuleValueId(values.len() as u32);
        values.push(value);
        id
    }

    pub(in crate::defuse) fn get(&self, id: RuleValueId) -> ClosedResidual {
        self.values.borrow()[id.0 as usize].clone()
    }

    /// A RESIDUAL CROSSES INTO THE RECEIVING USE: its record crosses into
    /// the receiver's, which decides from the two records alone what the
    /// capture replaces and which carrier the caller row became. What
    /// comes back is the residual as re-stored, and the row a later
    /// sibling stands over where the capture realized one.
    fn cross_into(
        &self,
        id: RuleValueId,
        bindings: &mut HoParamBindings,
        prepared: &mut Option<crate::defuse::carriers::CarrierRecord>,
        identities: &crate::relation::Planning,
    ) -> Result<(RuleValueId, Option<super::carriers::ResidualCaptureSource>)> {
        let mut residual = self.get(id);
        let receiver = prepared.get_or_insert_with(Default::default);
        let crossed = receiver.cross(&mut residual.prefix.carriers, bindings, identities)?;
        let id = if crossed.absorbs_join_input || crossed.moved_leading {
            self.insert(residual)
        } else {
            id
        };
        Ok((id, crossed.captured))
    }
}

impl HoSelection {
    fn name(&self) -> String {
        match self {
            HoSelection::Scoped(scoped) => scoped.name().to_string(),
            HoSelection::Family(family) => family.name().to_string(),
        }
    }

    fn params(&self) -> &[HoParam] {
        match self {
            HoSelection::Scoped(scoped) => scoped.params(),
            HoSelection::Family(family) => family.params(),
        }
    }

    /// The analyzed positions — a family fact for a consulted definition,
    /// a head analysis over the assembled clauses for a CHOE.
    fn positions(&self) -> Result<Vec<HoPositionInfo>> {
        match self {
            HoSelection::Scoped(scoped) => Ok(scoped.positions()),
            HoSelection::Family(family) => Ok(family.positions().to_vec()),
        }
    }

    fn output(&self) -> Result<crate::pipeline::asts::core::definitions::HeadItems> {
        let items = match self {
            HoSelection::Scoped(scoped) => scoped.head_items(),
            HoSelection::Family(family) => family.output().clone(),
        };
        Ok(structural_heading(&items))
    }
}

fn structural_heading(
    heading: &crate::pipeline::asts::core::definitions::HeadItems,
) -> crate::pipeline::asts::core::definitions::HeadItems {
    use crate::pipeline::asts::core::definitions::{HeadItem, HeadItems};
    match heading {
        HeadItems::Glob => HeadItems::Glob,
        HeadItems::Listed(items) => HeadItems::Listed(
            items
                .iter()
                .filter_map(|item| item.offered_name().cloned())
                .map(HeadItem::plumb)
                .collect(),
        ),
    }
}

fn residual_signature(
    selection: &HoSelection,
    cursor: usize,
) -> Result<crate::pipeline::asts::core::definitions::ResidualSignature> {
    use crate::pipeline::asts::core::definitions::{ResidualMode, ResidualSignature};
    let mut remaining = Vec::new();
    for param in &selection.params()[cursor..] {
        remaining.push(match param {
            HoParam::Relation { name, cols } => ResidualMode::Relation {
                name: name.clone(),
                cols: structural_heading(cols),
            },
            HoParam::Scalar { name, .. } | HoParam::Ground { name, .. } => {
                ResidualMode::Scalar { name: name.clone() }
            }
            HoParam::Rule { name, .. } => {
                return Err(DelightQLError::from(Ho::ResidualOrder {
                    message: format!(
                        "the residual of '{}' would still require rule-valued position '{name}', \
                         but the surface contract enumerates scalar and relation positions",
                        selection.name()
                    ),
                }));
            }
        });
    }
    Ok(ResidualSignature {
        remaining,
        output: selection.output()?,
    })
}

fn same_residual_contract(
    actual: &crate::pipeline::asts::core::definitions::ResidualSignature,
    expected: &crate::pipeline::asts::core::definitions::ResidualSignature,
) -> bool {
    actual.same_shape(expected)
}

/// Decode a rule designator without selecting it. The result is only the
/// authored naming plus its one prefix row; selection remains the shared
/// `select_ho` judgment.
fn designator_parts(
    chain: &ast_unresolved::Chain,
) -> Result<(SqlIdentifier, Vec<String>, ast_unresolved::CallArguments)> {
    use crate::pipeline::asts::core::{Access, GroundMention, Relation};
    if chain.has_steps() {
        return Err(DelightQLError::from(Ho::RuleValueForm {
    message: "a rule designator is one name and one optional configured prefix; relational continuations make a relation, not a rule value".to_string(),
}));
    }
    match chain.as_read_relation() {
        Some(Relation::FunctorCall { call, .. }) if !call.is_effect() => {
            if !matches!(chain.head_access(), Some(Access::Unasked)) {
                return Err(DelightQLError::from(Ho::RuleValueForm {
    message: "a trailing result-access group completes a relational application; raw repeated groups are not currying".to_string(),
}));
            }
            let call = call.call();
            Ok((
                call.callee.name_identifier(),
                call.callee.namespace_texts(),
                call.arguments.clone(),
            ))
        }
        Some(Relation::FunctorCall { .. }) => Err(DelightQLError::from(Ho::RuleValueEffect {
            message: "an effectful rule cannot inhabit a pure relational rule-value contract"
                .to_string(),
        })),
        Some(Relation::Ground {
            mention:
                GroundMention::Named {
                    identifier,
                    passthrough: false,
                    ..
                },
            ..
        }) => {
            let arguments = match chain.head_access() {
                Some(Access::All) => ast_unresolved::CallArguments::None,
                Some(Access::Slots(slots)) => {
                    use crate::pipeline::asts::core::operators::HoArgument;
                    ast_unresolved::CallArguments::higher_order(
                        slots
                            .iter()
                            .map(|slot| match slot.term() {
                                Some(term) => HoArgument::Value(
                                    crate::pipeline::asts::core::ArgumentValue::plain(term.clone()),
                                ),
                                None => HoArgument::Skip,
                            })
                            .collect(),
                    )
                }
                Some(_) | None => {
                    return Err(DelightQLError::from(Ho::RuleValueForm {
                        message: "this access form is relational, not a configured rule prefix"
                            .to_string(),
                    }))
                }
            };
            Ok((
                identifier.name.clone(),
                if identifier.namespace_path.is_empty() {
                    Vec::new()
                } else {
                    vec![identifier.namespace_path.to_string()]
                },
                arguments,
            ))
        }
        Some(_) | None => Err(DelightQLError::from(Ho::RuleValueForm {
            message: "this argument is a relation value, not a rule designator".to_string(),
        })),
    }
}

/// A RESIDUAL'S SEALED PREFIX JOINS THE SUFFIX SPEND: the prefix bound its
/// own formals through the authority, and no formal is bound in both.
fn merge_ho_bindings(target: &mut HoParamBindings, prefix: HoParamBindings) -> Result<()> {
    debug_assert!(prefix.formals.landed_source().is_none());
    target.formals.extend(prefix.formals)?;
    target.scalar_formals.extend(prefix.scalar_formals);
    target.scalar_literals.extend(prefix.scalar_literals);
    Ok(())
}

pub(in crate::defuse) fn construct_residual(
    designator: &ast_unresolved::Chain,
    expected: &crate::pipeline::asts::core::definitions::ResidualSignature,
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold<'_, '_>,
    mut evaluation_row: super::carriers::ResidualEvaluationRow<'_>,
) -> Result<RuleValueId> {
    use crate::pipeline::asts::core::operators::{CallArguments, HoArgument, ScalarArgument};
    let (name, namespace, arguments) = designator_parts(designator)?;

    // A formal crossing carries the already-closed value. It does not select
    // the same spelling in this body's declaration world.
    if namespace.is_empty() {
        if let Some(id) = fold.env.formal_rule(&name) {
            let residual = fold.core.residuals.get(id);
            if !same_residual_contract(&residual.signature, expected) {
                return Err(DelightQLError::from(Ho::ResidualContract {
                    message: format!(
                        "closed residual '{}' does not satisfy the receiving rule signature",
                        name
                    ),
                }));
            }
            return Ok(id);
        }
    }

    // `_` and `@` are present-row structure, never future holes. A callable
    // or enumeration likewise cannot stand in this relational prefix row.
    match &arguments {
        CallArguments::HigherOrder(part) => {
            for member in part.members().iter() {
                if matches!(
                    member,
                    HoArgument::Landing(_) | HoArgument::Skip | HoArgument::Landed(_)
                ) {
                    return Err(DelightQLError::from(Ho::ResidualPrefix {
    message: "a residual binds a complete left prefix; _, @ and a pipe landing do not defer a position".to_string(),
}));
                }
            }
        }
        CallArguments::Scalar(members)
            if members
                .iter()
                .any(|member| !matches!(member, ScalarArgument::Value(_))) =>
        {
            return Err(DelightQLError::from(Ho::ResidualPrefix {
                message: "a residual prefix contains only scalar and relation actuals".to_string(),
            }));
        }
        CallArguments::None | CallArguments::Scalar(_) => {}
    }

    let naming = if namespace.is_empty() {
        HoNaming::Enlisted
    } else {
        HoNaming::Qualified(&namespace)
    };
    let Some(selection) = select_ho(
        naming,
        name.as_str(),
        name.is_stropped(),
        fold.core,
        fold.env,
    )?
    else {
        return Err(DelightQLError::from(Ho::RuleValueMissing {
            message: format!(
                "'{}' does not name a pure parameterized relational rule",
                name
            ),
        }));
    };
    let (bindings, scalar_actuals, rules, prefix_scalar_spec, prepared_pipe, cursor) =
        split_ho_first_parens(
            &selection,
            &arguments,
            0,
            ParamRowCompletion::ProperPrefix,
            fold,
            evaluation_row.reborrow(),
        )?;
    if cursor >= selection.params().len() {
        return Err(DelightQLError::from(Ho::ResidualFrontier {
    message: format!(
                "'{}' supplies {} prefix actual(s) for {} declared position(s), leaving no residual",
                name,
                cursor,
                selection.params().len()
            ),
}));
    }
    let signature = residual_signature(&selection, cursor)?;
    if !same_residual_contract(&signature, expected) {
        return Err(DelightQLError::from(Ho::ResidualContract {
    message: format!(
                "the residual of '{}' has a different remaining mode or published heading than parameter '{}' requires",
                name,
                expected
                    .remaining
                    .first()
                    .map(|mode| mode.name().to_string())
                    .unwrap_or_else(|| "<empty>".to_string())
            ),
}));
    }

    let prepared = super::carriers::prepare_residual_prefix(
        bindings,
        scalar_actuals,
        rules,
        prepared_pipe,
        fold,
        evaluation_row,
    )?;
    let super::carriers::PreparedResidualPrefix {
        actuals: prefix,
        capture,
    } = prepared;
    let declared = selection.params().to_vec();
    let selection = match selection {
        HoSelection::Scoped(definition) => ClosedSelection::Scoped {
            definition,
            world: fold.env.close_lexical_world(),
        },
        HoSelection::Family(family) => ClosedSelection::Family(family),
    };
    let residual = ClosedResidual {
        declared,
        selection,
        cursor,
        signature,
        prefix,
        prefix_scalar_spec,
        capture,
    };
    Ok(fold.core.residuals.insert(residual))
}

fn merge_scalar_specs(
    left: ast_unresolved::Access,
    right: ast_unresolved::Access,
) -> ast_unresolved::Access {
    use crate::pipeline::asts::core::Access;
    let mut slots = Vec::new();
    if let Access::Slots(left) = left {
        slots.extend(left.into_vec());
    }
    if let Access::Slots(right) = right {
        slots.extend(right.into_vec());
    }
    match Vec1::try_from_vec(slots) {
        Some(slots) => Access::Slots(slots),
        None => Access::All,
    }
}

fn rule_capture_carriers(
    rules: &std::collections::HashMap<SqlIdentifier, RuleValueId>,
    residuals: &ResidualStore,
) -> Vec<crate::relation::PortId> {
    let mut tokens = Vec::new();
    for id in rules.values() {
        let residual = residuals.get(*id);
        let Some(capture) = residual.capture else {
            continue;
        };
        for carrier in capture.crossing {
            if !tokens.contains(&carrier) {
                tokens.push(carrier);
            }
        }
    }
    tokens
}

#[derive(Clone, Copy)]
enum CaptureBoundary {
    /// Another residual spend may still read the captured value.
    Crossing,
    /// The receiving consumer has spent every rule-valued formal.
    Spent,
}

fn carry_capture_tokens(
    resolved: ResolvedRelation,
    tokens: &[crate::relation::PortId],
    boundary: CaptureBoundary,
    alias: Option<&SqlIdentifier>,
    identities: &crate::relation::Planning,
) -> Result<ResolvedRelation> {
    if tokens.is_empty() {
        return Ok(resolved);
    }
    let input = resolved.semantic_relation();
    let ports = crate::relation::published_ports(identities, &input)?;
    let authority = identities.authority();
    if matches!(boundary, CaptureBoundary::Crossing) {
        for token in tokens {
            let count = ports
                .iter()
                .filter(|port| authority.residual_row_token(**port) == Some(*token))
                .count();
            if count != 1 {
                return Err(Internal::invariant(
                    "closed residual crossing",
                    "a residual spend did not publish its row token exactly once",
                ));
            }
        }
    }
    let sources: Vec<_> = ports
        .iter()
        .copied()
        .filter(|port| {
            let support = crate::relation::is_higher_order_support(identities, *port);
            match boundary {
                CaptureBoundary::Crossing => {
                    !support
                        || authority.residual_row_token(*port).is_some()
                        || authority.is_residual_capture_value(*port)
                }
                CaptureBoundary::Spent => !support,
            }
        })
        .collect();
    if sources == ports {
        return Ok(resolved);
    }
    let slots: Vec<_> = sources
        .iter()
        .copied()
        .map(|source| crate::relation::form::ProjectSlot::Carried {
            source,
            naming: crate::relation::form::Naming::Inherited,
        })
        .collect();
    let projected = resolved.republished(|chain| {
        authority.extend(
            chain,
            crate::relation::builder::StepOp::Republish {
                of: crate::relation::builder::Republishing::Project(
                    crate::relation::form::ProjectSpec {
                        input,
                        why: crate::relation::form::ProjectWhy::Restate,
                        slots: &slots,
                    },
                ),
                sources,
            },
        )
    })?;
    match alias {
        Some(alias) => {
            let spelling = identities.intern(alias.as_str(), alias.is_stropped());
            projected.aliased(spelling, identities)
        }
        None => Ok(projected),
    }
}

#[allow(clippy::too_many_arguments)]
fn spend_residual(
    residual: ClosedResidual,
    access: &ast_unresolved::Access,
    arguments: &ast_unresolved::CallArguments,
    caller_row: &mut crate::pipeline::resolver::CallerRow,
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold<'_, '_>,
    alias: Option<SqlIdentifier>,
) -> Result<ResolvedRelation> {
    let tokens = residual
        .capture
        .as_ref()
        .map(|capture| vec![capture.row_token])
        .unwrap_or_default();
    let crossing = residual
        .capture
        .as_ref()
        .map(|capture| capture.crossing.clone())
        .unwrap_or_default();
    let function = residual.selection.name();
    let (selection, scoped_world) = residual.selection.open();
    let (bindings, scalar_actuals, rules, suffix_scalar_spec, prepared_pipe, _) =
        split_ho_first_parens(
            &selection,
            arguments,
            residual.cursor,
            ParamRowCompletion::CompleteThrough(residual.declared.len()),
            fold,
            super::carriers::ResidualEvaluationRow::Caller(caller_row),
        )?;
    let scalar_spec = merge_scalar_specs(residual.prefix_scalar_spec, suffix_scalar_spec);
    let user_alias = alias.clone();
    let resolved = expand_ho_view(
        &function,
        selection,
        &scalar_spec,
        access,
        bindings,
        scalar_actuals,
        rules,
        &crossing,
        Some(residual.prefix),
        prepared_pipe,
        scoped_world,
        caller_row,
        fold,
        alias,
    )?;
    carry_capture_tokens(
        resolved,
        &tokens,
        CaptureBoundary::Crossing,
        user_alias.as_ref(),
        &fold.core.identities,
    )
}

/// Select one higher-order definition through the existing exhaustive
/// definition-use authority. Invocation and residual construction share this
/// exact judgment; neither may reimplement a name lookup.
fn select_ho(
    naming: HoNaming<'_>,
    function: &str,
    function_stropped: bool,
    core: &mut crate::resolution::ResolverCore<'_>,
    env: &mut super::environment::Environment,
) -> Result<Option<HoSelection>> {
    use crate::enums::EntityType;
    let selection = match naming {
        HoNaming::Qualified(ns) => {
            let fq = ns.join("::");
            let selected = core
                .consult
                .select_entity(function, function_stropped, &fq, env.reach())?
                .unique_or_refuse(function)?
                .and_then(|selected| match selected {
                    super::select::Selected::Authored(family)
                        if family.kind() == EntityType::DqlHoTemporaryViewExpression =>
                    {
                        Some(family)
                    }
                    _ => None,
                });
            let Some(entity) = selected else {
                return Ok(None);
            };
            HoSelection::Family(super::admitted::ClosedHoFamily::close(entity)?)
        }
        HoNaming::Enlisted => {
            let bare = if function_stropped {
                SqlIdentifier::stropped(function)
            } else {
                SqlIdentifier::new(function)
            };
            match env.select_query_local(
                &bare,
                crate::pipeline::asts::core::QueryLocalDemand::HigherOrder,
                None,
            )? {
                Some(super::environment::QueryLocalSelection::HigherOrder(definition)) => {
                    HoSelection::Scoped(definition)
                }
                Some(_) => unreachable!("higher-order demand returns only a CHOE"),
                None => {
                    let Some(entity) = super::bound_use::select_enlisted_ho(
                        core,
                        env,
                        function,
                        function_stropped,
                    )?
                    else {
                        return Ok(None);
                    };
                    HoSelection::Family(super::admitted::ClosedHoFamily::close(entity)?)
                }
            }
        }
    };
    Ok(Some(selection))
}

/// THE ONE HIGHER-ORDER USE ENTRANCE. Selects the family for the naming
/// (kind judged over the complete candidate set, never probe order),
/// derives the grounding the declaration's own namespace declares, binds
/// the caller-resolved actuals to the declared formals (the final-landing
/// law), admits the instance under the semantic actual key, opens and
/// squishes the body, and resolves it in the declaration environment.
/// `Ok(None)` is a position miss — the caller's ladder (tables, TVFs)
/// stands; the definition pieces never leave this module.
#[allow(clippy::too_many_arguments)]
pub(crate) fn use_ho_invocation(
    naming: HoNaming<'_>,
    function: &str,
    function_stropped: bool,
    access: &ast_unresolved::Access,
    // THE AUTHORED ROW, WHOLE — the landed member among the others, exactly
    // as the build left it. No road can select a family, take a source, and
    // go looking for a formal to put it in: the source is already at one.
    arguments: &ast_unresolved::CallArguments,
    caller_row: &mut crate::pipeline::resolver::CallerRow,
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold<'_, '_>,
    alias: Option<SqlIdentifier>,
) -> Result<Option<ResolvedRelation>> {
    if matches!(&naming, HoNaming::Enlisted) {
        let formal = if function_stropped {
            SqlIdentifier::stropped(function)
        } else {
            SqlIdentifier::new(function)
        };
        if let Some(id) = fold.env.formal_rule(&formal) {
            let residual = fold.core.residuals.get(id);
            return spend_residual(residual, access, arguments, caller_row, fold, alias).map(Some);
        }
    }
    let Some(selection) = select_ho(naming, function, function_stropped, fold.core, fold.env)?
    else {
        return Ok(None);
    };
    let (table_bindings, scalar_actuals, rule_actuals, scalar_spec, prepared_pipe, _) =
        split_ho_first_parens(
            &selection,
            arguments,
            0,
            ParamRowCompletion::CompleteThrough(selection.params().len()),
            fold,
            super::carriers::ResidualEvaluationRow::Caller(caller_row),
        )?;
    let carriers = rule_capture_carriers(&rule_actuals, &fold.core.residuals);
    let user_alias = alias.clone();
    let resolved = expand_ho_view(
        function,
        selection,
        &scalar_spec,
        access,
        table_bindings,
        scalar_actuals,
        rule_actuals,
        &carriers,
        None,
        prepared_pipe,
        None,
        caller_row,
        fold,
        alias,
    )?;
    Ok(Some(carry_capture_tokens(
        resolved,
        &carriers,
        CaptureBoundary::Spent,
        user_alias.as_ref(),
        &fold.core.identities,
    )?))
}

#[cfg(test)]
mod ground_number_equality_tests {
    use super::normalize_number;

    fn eq(a: &str, b: &str) -> bool {
        normalize_number(a) == normalize_number(b) && normalize_number(a).is_some()
    }

    #[test]
    fn equal_values_across_spellings() {
        assert!(eq("12", "12.0"));
        assert!(eq("12", "1.2e1"));
        assert!(eq("0.5", "5e-1"));
        assert!(eq("0", "-0"));
        assert!(eq("0", "0.000"));
        assert!(eq("-3.25", "-32.5e-1"));
        assert!(eq("042", "42"));
    }

    #[test]
    fn adjacent_integers_beyond_f64_stay_distinct() {
        // 2^53 and 2^53 + 1: identical as f64, distinct as integers.
        assert!(!eq("9007199254740992", "9007199254740993"));
        assert!(!eq("-9007199254740992", "-9007199254740993"));
        // And the honest positive control at the same magnitude.
        assert!(eq("9007199254740993", "9007199254740993.0"));
    }

    #[test]
    fn sign_and_magnitude_matter() {
        assert!(!eq("1", "-1"));
        assert!(!eq("10", "1"));
        assert!(!eq("0.1", "0.01"));
    }

    #[test]
    fn non_numeric_spellings_do_not_normalize() {
        assert!(normalize_number("abc").is_none());
        assert!(normalize_number("").is_none());
        assert!(normalize_number("1.2.3").is_none());
        assert!(normalize_number("1e").is_none());
    }

    /// A ground head is a match arm, not join correspondence.
    ///
    /// The caller's row value is tested against the clause's declared ground
    /// value before the clauses accumulate. In particular, a `null` actual
    /// selects a `null` ground head. SQL `formal = NULL` silently empties the
    /// clause instead; the comparison must retain the language's null-safe
    /// ground-match law.
    #[test]
    fn a_null_ground_head_builds_a_null_safe_dispatch_match() {
        use super::{ground_scalar_correlation_condition, HoParam, HoPositionInfo};
        use crate::pipeline::asts::ddl::{HoColumnKind, HoGroundPattern};
        use crate::pipeline::asts::unresolved;
        use crate::pipeline::asts::vocabulary::CmpOp;

        let params = vec![HoParam::Ground {
            name: delightql_types::SqlIdentifier::new("choice"),
            text: "null".into(),
        }];
        let positions = vec![HoPositionInfo {
            position: 0,
            column_kind: HoColumnKind::Scalar,
            ground_pattern: Some(HoGroundPattern::SomeClauses),
            ground_values: vec![(0, "null".into())],
            column_name: Some("choice".into()),
        }];
        let mut authored_bare = std::collections::HashMap::new();
        authored_bare.insert("choice".into(), "choice".into());
        let actuals = crate::defuse::bound_use::HoActuals {
            carriers: Default::default(),
            bindings: Default::default(),
            values: Default::default(),
            authored_bare,
            rules: Default::default(),
        };

        let condition = ground_scalar_correlation_condition(&params, &positions, &actuals)
            .expect("the ground clause has one caller-borne discriminator");
        let unresolved::TruthExpression::Comparison(crate::pipeline::asts::core::Comparison {
            operator,
            ..
        }) = condition
        else {
            panic!("one ground position produces one comparison")
        };
        assert_eq!(
            operator,
            CmpOp::NullSafeEqual,
            "a null ground head must match a null caller value"
        );
    }
}
