// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::ProbeAddressing;
use crate::pipeline::asts::core::{Comparison, Existence};
use crate::pipeline::asts::core::{NamedReference, Reference};
use delightql_types::SqlIdentifier;

// =============================================================================
// USING correlation synthesis for semi-joins
// =============================================================================

/// Wrap a resolved subquery with correlation predicates derived from USING columns.
/// For `+orders(*.(status))`, this produces:
///   Filter(subquery, outer.status IS NOT DISTINCT FROM orders.status)
pub(in crate::pipeline::resolver) fn synthesize_using_correlation(
    subquery: ast_resolved::Chain,
    using_columns: &[SqlIdentifier],
    outer_available: &[crate::relation::PortId],
    identities: &crate::relation::Planning,
) -> Result<ast_resolved::Chain> {
    use crate::pipeline::asts::core::FilterOrigin;

    if using_columns.is_empty() {
        return Ok(subquery);
    }

    let inner_schema = subquery.semantic_relation();
    let inner = crate::relation::published_ports(identities, &inner_schema)?;

    // Build one comparison per USING column
    let mut comparisons: Vec<ast_resolved::TruthExpression> = Vec::new();
    for col_name in using_columns {
        // As written — a strop makes the name case-sensitive, and the lvar
        // this step unifies with is the one the author spelled.
        let spelling = identities.intern(col_name.as_str(), col_name.is_stropped());
        let name = identities.canonical(spelling);
        let outer_hits: Vec<_> = outer_available
            .iter()
            .copied()
            .filter(|column| identities.published_sym(column.column()) == Some(name))
            .collect();
        let inner_hits: Vec<_> = inner
            .iter()
            .copied()
            .filter(|column| identities.published_sym(column.column()) == Some(name))
            .collect();
        let outer = unique_using_column(col_name, "outer", &outer_hits)?;
        let inner = unique_using_column(col_name, "inner", &inner_hits)?;
        let lhs = ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence::engine(outer),
        )));
        let rhs = ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence::engine(inner),
        )));

        comparisons.push(ast_resolved::TruthExpression::Comparison(Comparison {
            operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
            left: Box::new(lhs),
            right: Box::new(rhs),
        }));
    }

    // Combine with AND
    let combined = ast_resolved::TruthExpression::all(comparisons)
        .expect("a non-empty USING list produces one comparison per column");

    // Wrap subquery in Filter. A filter publishes its source's heading, and
    // this one is built in the resolved phase, so it carries that heading
    // rather than leaving a phantom for a later phase to fill: nothing runs
    // between here and the refiner that would.
    Ok(subquery.transparently(ast_resolved::Transparent::Restrict {
        condition: combined,
        origin: FilterOrigin::Generated,
    }))
}

fn unique_using_column(
    name: &SqlIdentifier,
    side: &str,
    hits: &[crate::relation::PortId],
) -> Result<crate::relation::PortId> {
    match hits {
        [column] => Ok(*column),
        [] => Err(DelightQLError::column_not_found_error(
            name.as_str(),
            format!("in {side} heading for USING correlation"),
        )),
        _ => Err(DelightQLError::validation_error_categorized(
            "resolution/ambiguous",
            format!("USING column '{name}' appears more than once in the {side} heading"),
            "publish a unique name on each side before correlating",
        )),
    }
}

/// Build individual correlation SigmaConditions from USING columns.
/// Returns one SigmaCondition per column (not combined with AND), so that
/// `insert_filter_at_base` can wrap them as separate Filter nodes.
/// This matches the structure the explicit comma path produces, which the
/// CDT-SJ classifier and hygienic injection mechanism expect.
pub(in crate::pipeline::resolver) fn build_using_correlation_filters(
    using_columns: &[SqlIdentifier],
    outer_available: &[crate::relation::PortId],
    inner_expression: &ast_resolved::Chain,
    identities: &crate::relation::Planning,
) -> Result<Vec<ast_resolved::TruthExpression>> {
    let inner_relation = inner_expression.semantic_relation();
    let inner = crate::relation::published_ports(identities, &inner_relation)?;

    using_columns
        .iter()
        .map(|col_name| {
            let spelling = identities.intern(col_name.as_str(), col_name.is_stropped());
            let name = identities.canonical(spelling);
            let outer: Vec<_> = outer_available
                .iter()
                .copied()
                .filter(|column| identities.published_sym(column.column()) == Some(name))
                .collect();
            let inner: Vec<_> = inner
                .iter()
                .copied()
                .filter(|column| identities.published_sym(column.column()) == Some(name))
                .collect();
            let outer = unique_using_column(col_name, "outer", &outer)?;
            let inner = unique_using_column(col_name, "inner", &inner)?;
            let lhs = ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence::engine(outer),
            )));
            let rhs = ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence::engine(inner),
            )));

            Ok(ast_resolved::TruthExpression::Comparison(Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
                left: Box::new(lhs),
                right: Box::new(rhs),
            }))
        })
        .collect()
}

/// The filters `.*` asks for: one per name BOTH sides publish.
///
/// The same question the join spelling asks, answered by the same
/// computation — so a heading pair that refuses at a join refuses here, and
/// neither placement can quietly perform a step the other rejects.
pub(in crate::pipeline::resolver) fn build_using_all_correlation_filters(
    outer_available: &[crate::relation::PortId],
    inner_expression: &ast_resolved::Chain,
    identities: &crate::relation::Planning,
) -> Result<Vec<ast_resolved::TruthExpression>> {
    let inner_relation = inner_expression.semantic_relation();
    let inner = crate::relation::published_ports(identities, &inner_relation)?;

    Ok(
        crate::pipeline::resolver::lexical::shared_using_names(
            outer_available,
            &inner,
            identities,
        )?
        .into_iter()
        .map(|shared| {
            ast_resolved::TruthExpression::Comparison(Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
                left: Box::new(ast_resolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(ColumnOccurrence::engine(shared.left)),
                ))),
                right: Box::new(ast_resolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(ColumnOccurrence::engine(shared.right)),
                ))),
            })
        })
        .collect(),
    )
}

// =============================================================================
// Destructuring Pattern Helpers (Epoch 2)
// =============================================================================

/// The JSON key → published-name mappings an UNRESOLVED pattern declares.
///
/// A pattern member says what it binds; nothing here re-derives that from a
/// value's shape, because a pattern member holds no value.
pub(crate) fn extract_key_mappings_from_unresolved_pattern(
    pattern: &ast_unresolved::TreePattern,
) -> Result<Vec<(String, String)>> {
    let mut mappings = Vec::new();
    collect_key_mappings(pattern, &mut mappings)?;
    Ok(mappings)
}

fn collect_key_mappings(
    pattern: &ast_unresolved::TreePattern,
    mappings: &mut Vec<(String, String)>,
) -> Result<()> {
    use crate::pipeline::asts::core::{PatternTarget, RecordPatternMember, TreePattern};
    match pattern {
        TreePattern::Record(record) => {
            for member in record.members.iter() {
                match member {
                    // A binder's key IS its name.
                    RecordPatternMember::Binder(binder) => {
                        mappings.push((binder.name.to_string(), binder.name.to_string()));
                    }
                    // A rename: the JSON key on the left, the published name
                    // on the right.
                    RecordPatternMember::Keyed { key, binder } => {
                        mappings.push((key.clone(), binder.name.to_string()));
                    }
                    // A nested level binds nothing of its own; its members do.
                    RecordPatternMember::Nested { pattern, .. } => {
                        collect_key_mappings(pattern, mappings)?
                    }
                    RecordPatternMember::Path(binding) => {
                        mappings.push((binding.path.mapping_key(), binding.published_name()));
                    }
                    // The KEYS become this column's values, and the target's
                    // members bind under them.
                    RecordPatternMember::Metadata { key, target } => {
                        mappings.push((key.name.to_string(), key.name.to_string()));
                        if let PatternTarget::Pattern(inner) = target {
                            collect_key_mappings(inner, mappings)?;
                        }
                    }
                    // The anaphor iterates and binds nothing.
                    RecordPatternMember::Disregarded => {}
                }
            }
        }
        TreePattern::Array(array) => {
            for member in array.members.iter() {
                mappings.push((member.path.mapping_key(), member.published_name()));
            }
        }
    }
    Ok(())
}

/// Validate UNRESOLVED pattern is appropriate for the destructuring mode
pub(in crate::pipeline::resolver) fn validate_unresolved_pattern_for_mode(
    _pattern: &ast_unresolved::TreePattern,
    mode: &ast_unresolved::DestructureMode,
) -> Result<()> {
    use ast_unresolved::DestructureMode;

    match mode {
        DestructureMode::Scalar => {
            // Scalar mode WITH nested explosions is allowed
        }
        DestructureMode::Aggregate => {
            // Aggregate mode - nested explosions are allowed
        }
    }
    Ok(())
}

/// Refuse a destructure pattern that binds the same column name more
/// than once — at any level. The bindings share one flat output
/// heading, so a duplicate is not two extractions: one silently
/// overwrites the other and the loser's value is unobservable.
pub(in crate::pipeline::resolver) fn validate_distinct_bindings(
    pattern: &ast_unresolved::TreePattern,
) -> Result<()> {
    let mut bindings: Vec<delightql_types::SqlIdentifier> = Vec::new();
    collect_pattern_bindings(pattern, &mut bindings)
}

fn bind_pattern_name(
    name: &delightql_types::SqlIdentifier,
    seen: &mut Vec<delightql_types::SqlIdentifier>,
) -> Result<()> {
    if seen.contains(name) {
        return Err(DelightQLError::validation_error(
            format!(
                "destructure pattern binds '{}' more than once — the bindings \
                 share one output heading, so one extraction silently overwrites \
                 the other. Give each a distinct column name with an explicit \
                 key: \"{}\": other_name",
                name, name
            ),
            "destructuring",
        ));
    }
    seen.push(name.clone());
    Ok(())
}

fn collect_pattern_bindings(
    pattern: &ast_unresolved::TreePattern,
    seen: &mut Vec<delightql_types::SqlIdentifier>,
) -> Result<()> {
    use crate::pipeline::asts::core::{PatternTarget, RecordPatternMember, TreePattern};
    match pattern {
        TreePattern::Record(record) => {
            for member in record.members.iter() {
                match member {
                    RecordPatternMember::Binder(binder) => bind_pattern_name(&binder.name, seen)?,
                    RecordPatternMember::Keyed { binder, .. } => {
                        bind_pattern_name(&binder.name, seen)?
                    }
                    RecordPatternMember::Nested { pattern, .. } => {
                        collect_pattern_bindings(pattern, seen)?
                    }
                    RecordPatternMember::Path(binding) => {
                        if let Some(name) = path_binding_name(&binding.path, &binding.naming) {
                            bind_pattern_name(&name, seen)?
                        }
                    }
                    RecordPatternMember::Metadata { key, target } => {
                        bind_pattern_name(&key.name, seen)?;
                        if let PatternTarget::Pattern(inner) = target {
                            collect_pattern_bindings(inner, seen)?;
                        }
                    }
                    RecordPatternMember::Disregarded => {}
                }
            }
            Ok(())
        }
        TreePattern::Array(array) => {
            for member in array.members.iter() {
                if let Some(name) = path_binding_name(&member.path, &member.naming) {
                    bind_pattern_name(&name, seen)?
                }
            }
            Ok(())
        }
    }
}

/// The output column a reaching member binds: its name, or the path's last
/// object key. Index-terminated unnamed paths derive positional names
/// elsewhere and cannot collide by spelling.
fn path_binding_name(
    path: &crate::pipeline::asts::core::Path,
    naming: &Option<delightql_types::SqlIdentifier>,
) -> Option<delightql_types::SqlIdentifier> {
    if let Some(a) = naming {
        return Some(a.clone());
    }
    path.last_key().map(delightql_types::SqlIdentifier::from)
}

/// Refuse sibling explosions: two `~>` at one pattern level would multiply
/// against each other, and which product the author meant is unstated.
pub(in crate::pipeline::resolver) fn validate_no_sibling_explosions(
    pattern: &ast_unresolved::TreePattern,
) -> Result<()> {
    use crate::pipeline::asts::core::{PatternTarget, RecordPatternMember, TreePattern};
    let TreePattern::Record(record) = pattern else {
        // A positional pattern binds indices; no member of one explodes.
        return Ok(());
    };
    let explosion_count = record
        .members
        .iter()
        .filter(|member| {
            matches!(
                member,
                RecordPatternMember::Nested {
                    iteration: true,
                    ..
                } | RecordPatternMember::Metadata { .. }
            )
        })
        .count();
    if explosion_count > 1 {
        return Err(DelightQLError::validation_error(
            "Multiple array explosions (~>) at the same pattern level create ambiguous cartesian product.\n\
             Use sequential steps instead:\n\
             Example:\n\
             - Step 1: data ~= ~> {{\"users\": users_data, \"orders\": orders_data}}\n\
             - Step 2: users_data ~= ~> {{first_name}}",
            "destructuring"
        ));
    }
    for member in record.members.iter() {
        match member {
            RecordPatternMember::Nested { pattern, .. } => validate_no_sibling_explosions(pattern)?,
            RecordPatternMember::Metadata {
                target: PatternTarget::Pattern(inner),
                ..
            } => validate_no_sibling_explosions(inner)?,
            RecordPatternMember::Binder(_)
            | RecordPatternMember::Keyed { .. }
            | RecordPatternMember::Path(_)
            | RecordPatternMember::Metadata {
                target: PatternTarget::Disregarded,
                ..
            }
            | RecordPatternMember::Disregarded => {}
        }
    }
    Ok(())
}

/// Bind an unresolved pattern's binders to the occurrences the destructure
/// minted for them. Keys, reaches and iteration marks are spec material and
/// cross unchanged.
pub(crate) fn convert_destructure_pattern_to_resolved(
    pattern: ast_unresolved::TreePattern,
    columns: &std::collections::HashMap<crate::names::Sym, crate::relation::PortId>,
    identities: &crate::names::Registry,
) -> Result<ast_resolved::TreePattern> {
    use crate::pipeline::asts::core::{PatternTarget, RecordPatternMember, TreePattern};
    Ok(match pattern {
        TreePattern::Record(record) => TreePattern::Record(ast_resolved::RecordPattern {
            members: record.members.try_map(|member| -> Result<_> {
                Ok(match member {
                    RecordPatternMember::Binder(binder) => RecordPatternMember::Binder(
                        destructure_column(binder.name.as_str(), columns, identities)?,
                    ),
                    RecordPatternMember::Keyed { key, binder } => RecordPatternMember::Keyed {
                        key,
                        binder: destructure_column(binder.name.as_str(), columns, identities)?,
                    },
                    RecordPatternMember::Nested {
                        key,
                        iteration,
                        pattern,
                    } => RecordPatternMember::Nested {
                        key,
                        iteration,
                        pattern: Box::new(convert_destructure_pattern_to_resolved(
                            *pattern, columns, identities,
                        )?),
                    },
                    RecordPatternMember::Path(binding) => RecordPatternMember::Path(binding),
                    RecordPatternMember::Metadata { key, target } => {
                        RecordPatternMember::Metadata {
                            key: destructure_column(key.name.as_str(), columns, identities)?,
                            target: match target {
                                PatternTarget::Pattern(inner) => PatternTarget::Pattern(Box::new(
                                    convert_destructure_pattern_to_resolved(
                                        *inner, columns, identities,
                                    )?,
                                )),
                                PatternTarget::Disregarded => PatternTarget::Disregarded,
                            },
                        }
                    }
                    RecordPatternMember::Disregarded => RecordPatternMember::Disregarded,
                })
            })?,
        }),
        // Members are paths and names — resolution has nothing to decide
        // about either.
        TreePattern::Array(array) => TreePattern::Array(array),
    })
}

fn destructure_column(
    name: &str,
    columns: &std::collections::HashMap<crate::names::Sym, crate::relation::PortId>,
    identities: &crate::names::Registry,
) -> Result<crate::relation::PortId> {
    let spelling = identities.intern(name, false);
    columns
        .get(&identities.canonical(spelling))
        .copied()
        .ok_or_else(|| {
            DelightQLError::parse_error(
                "destructuring pattern output has no structural column occurrence",
            )
        })
}

/// Expand a table (fact) used as a sigma predicate.
///
/// `+no_data(x)` expands to:
///   EXISTS (SELECT * FROM no_data AS _fact WHERE x IS NOT DISTINCT FROM _fact.|1|)
///
/// `outer_columns` is the scope of the CALLING clause; argument lvars are
/// bound against it so the correlation survives resolution inside EXISTS.
///
/// Constructs the AST directly without re-parsing.
pub(in crate::pipeline::resolver) fn expand_table_as_sigma(
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold<'_, '_>,
    table_name: &str,
    namespace: Vec<String>,
    arguments: Vec<ast_unresolved::DomainExpression>,
    polarity: crate::pipeline::asts::core::Polarity,
) -> Result<ast_resolved::TruthExpression> {
    use crate::pipeline::ast_transform::AstTransform;

    if arguments.is_empty() {
        return Err(DelightQLError::parse_error(format!(
            "Sigma predicate '+{}()' requires at least one argument",
            table_name
        )));
    }

    // The arguments are written in the ENCLOSING clause, so they are resolved
    // here, standing in it — before anything nests. Resolving them after the
    // fact relation is in scope is how a shared name came to compare the fact
    // row with itself, and pre-binding them into the authored tree was the
    // workaround.
    let resolved_arguments = arguments
        .into_iter()
        .map(|argument| fold.transform_domain(argument))
        .collect::<Result<Vec<_>>>()?;

    // A QUALIFIED citation (`+HL.h(v)`) stamps its qualifier here, so the
    // inner reference resolves through the qualified-relation machinery —
    // aliases (session or scope-local), exposure, and its refusals.
    let table_ident = ast_unresolved::QualifiedName {
        namespace_path: crate::pipeline::asts::core::metadata::NamespacePath::from_parts(namespace)
            .expect("qualified sigma namespace is nonempty"),
        name: table_name.into(),
    };

    let fact_alias = "_fact";

    // Build subquery: table_name(*) as _fact
    let subquery = ast_unresolved::Chain::read(
        ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named {
                identifier: table_ident.clone(),
                alias: Some(fact_alias.into()),
                mutation_target: false,
                passthrough: false,
            },
            outer: false,
        },
        ast_unresolved::Access::All,
    );

    // The fact relation resolves on its own, through the ordinary
    // qualified-relation machinery — aliases, exposure, and its refusals.
    let resolved =
        fold.transform_boolean(ast_unresolved::TruthExpression::Existence(Existence {
            polarity,
            relation: Box::new(subquery),
            addressing: ProbeAddressing {
                identifier: table_ident,
                using_columns: vec![],
            },
        }))?;
    let ast_resolved::TruthExpression::Existence(Existence {
        polarity,
        relation: subquery,
        ..
    }) = resolved
    else {
        return Err(DelightQLError::transformation_error(
            "resolving a sigma predicate's fact relation did not produce a \
             membership test",
            "sigma_expansion",
        ));
    };

    // The guard is synthesized AFTER both sides are resolved, so each side is
    // an occurrence its own scope answered for. Argument `i` constrains
    // dimension `i` of the fact relation: the correlation is positional, which
    // is what a fact's argument list means.
    let subquery =
        synthesize_argument_correlation(*subquery, resolved_arguments, &fold.core.identities)?;

    Ok(ast_resolved::TruthExpression::Existence(Existence {
        polarity,
        relation: Box::new(subquery),
        addressing: (),
    }))
}

/// Constrain a fact relation's dimensions by the arguments the caller wrote.
///
/// Both sides arrive resolved: the arguments were answered by the enclosing
/// clause and the dimensions by the fact relation. Nothing here addresses
/// anything by characters, so a fact that publishes the same name as an
/// argument cannot capture it.
fn synthesize_argument_correlation(
    subquery: ast_resolved::Chain,
    arguments: Vec<ast_resolved::DomainExpression>,
    identities: &crate::relation::Planning,
) -> Result<ast_resolved::Chain> {
    use crate::pipeline::asts::core::FilterOrigin;

    let fact_scope = subquery.semantic_relation();
    let dimensions = crate::relation::published_ports(identities, &fact_scope)?;
    if dimensions.len() < arguments.len() {
        return Err(DelightQLError::validation_error(
            format!(
                "a fact taking {} arguments has only {} dimensions",
                arguments.len(),
                dimensions.len()
            ),
            "in a sigma predicate",
        ));
    }

    let combined = arguments
        .into_iter()
        .zip(dimensions.iter().copied())
        .map(|(argument, dimension)| {
            ast_resolved::TruthExpression::Comparison(Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
                left: Box::new(argument),
                right: Box::new(ast_resolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(ColumnOccurrence::engine_qualified(dimension)),
                ))),
            })
        })
        .collect::<Vec<_>>();
    let combined = ast_resolved::TruthExpression::all(combined)
        .expect("a sigma predicate refuses an empty argument list");

    Ok(subquery.transparently(ast_resolved::Transparent::Restrict {
        condition: combined,
        origin: FilterOrigin::Generated,
    }))
}

#[cfg(test)]
mod sigma_argument_tests {
    use super::*;
    use crate::pipeline::asts::core::Access;

    fn fact_scope(
        registry: &crate::relation::Planning,
        name: &str,
        columns: &[&str],
    ) -> crate::relation::SemanticRelation {
        let answer = registry.intern(name, false);
        let entity = registry.mint_entity(answer);
        let slots: Vec<_> = columns
            .iter()
            .enumerate()
            .map(|(position, column)| crate::relation::form::SourceSlot {
                position: position as u32,
                named: Some(registry.intern(column, false)),
                declared_type: None,
            })
            .collect();
        registry
            .authority()
            .derive(crate::relation::RelForm::Source(
                crate::relation::form::SourceSpec {
                    origin: crate::relation::form::SourceOrigin::Catalog { entity },
                    slots: &slots,
                    answers_to: Some(answer),
                },
            ))
            .unwrap()
    }

    fn fact_chain(
        registry: &crate::relation::Planning,
        scope: crate::relation::SemanticRelation,
    ) -> ast_resolved::Chain {
        registry
            .authority()
            .ground_read(Access::All, false, scope)
            .unwrap()
    }

    fn outer_column(registry: &crate::relation::Planning, name: &str) -> crate::relation::PortId {
        let relation = fact_scope(registry, "outer", &[name]);
        crate::relation::published_ports(registry, &relation).unwrap()[0]
    }

    fn argument(column: crate::relation::PortId) -> ast_resolved::DomainExpression {
        ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence::engine(column),
        )))
    }

    /// The guard names the fact's OWN dimension, positionally — never a
    /// column it found by matching the argument's name. A fact publishing
    /// the same name as the argument is the case the old pre-binding pass
    /// existed to survive.
    #[test]
    fn an_argument_constrains_the_dimension_at_its_position() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let scope = fact_scope(&registry, "no_data", &["x", "y"]);
        let dimensions = crate::relation::published_ports(&registry, &scope).unwrap();
        let outer = outer_column(&registry, "x");

        let guarded = synthesize_argument_correlation(
            fact_chain(&registry, scope),
            vec![argument(outer)],
            &registry,
        )
        .expect("one argument, two dimensions");

        let ast_resolved::Continuation::Restrict {
            condition: predicate,
            ..
        } = guarded
            .continuations()
            .last()
            .expect("a guard was appended")
            .form()
        else {
            panic!("expected a restriction carrying the guard");
        };
        let ast_resolved::TruthExpression::Comparison(Comparison { left, right, .. }) = predicate
        else {
            panic!("expected one comparison");
        };
        assert_eq!(**left, argument(outer), "the argument stays the caller's");
        let ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) = &**right
        else {
            panic!("expected the fact's dimension");
        };
        assert_eq!(
            *column, dimensions[0],
            "argument 0 constrains dimension 0, by position"
        );
        assert_ne!(
            *column, outer,
            "the fact's `x` never captures the outer `x`"
        );
    }

    /// A fact with fewer dimensions than the caller wrote arguments for is
    /// refused, not silently truncated by the zip.
    #[test]
    fn more_arguments_than_dimensions_is_refused() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let scope = fact_scope(&registry, "no_data", &["x"]);
        let first = outer_column(&registry, "a");
        let second = outer_column(&registry, "b");

        assert!(synthesize_argument_correlation(
            fact_chain(&registry, scope),
            vec![argument(first), argument(second)],
            &registry,
        )
        .is_err());
    }
}
