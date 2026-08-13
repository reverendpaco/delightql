// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Mutation lowering — one road, no private entrance.
//!
//! A mutation terminal is an ordinary continuation consumer. It receives what
//! every other consumer receives: the relation-so-far, lowered, as one opaque
//! operand. The operand offers exactly two things — the heading it publishes
//! and the query it becomes — and offers them to everyone alike. There is no
//! accessor for "the source's WHERE" and none for "the source's select list",
//! so nothing here can decide between two roads by probing the shape of the
//! SQL underneath, and a restriction the source states has nowhere to be
//! left behind.
//!
//! What that costs is one indirection: the rows a mutation touches are the
//! rows its source publishes, matched by the target's own columns, and the
//! values it writes are read out of that same source. What it buys is that a
//! join, a projection, a name, a bound, or a shape nobody has thought of yet
//! all reach the statement the same way — whole.

use crate::error::{DelightQLError, Result};
use crate::names::{ColId, ScopeId};
use crate::pipeline::asts::core::ColumnMetadata;
use crate::pipeline::asts::refined as ast_refined;
use crate::pipeline::sql_ast::statements::RelationTarget;
use crate::pipeline::sql_ast::{
    Cte, DomainExpression, QueryExpression, SelectItem, SelectStatement, SqlStatement,
    TableExpression,
};

use super::builder::{Builder, NameGenerator, Projected};
use super::relational;
use super::{descend, Lowered, Mutation, Obligation, TransformCtx};

/// The operand a mutation consumes: the shared lowered relation, entire.
type Operand = Builder<Projected>;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Transform a query whose innermost relational expression is a mutation
/// call, or hand the query back UNTOUCHED when it is not one.
///
/// THE PARTITION IS THE MEMBERSHIP: `take_mutation` either returns the
/// mutation's parts or returns the chain unchanged, so there is no boolean
/// detector for this consumer to disagree with and no re-destructure to
/// panic in.
///
/// Query wrappers are handled the way `transform_with_names` handles them;
/// the chain underneath goes to the one mutation road.
#[stacksafe::stacksafe]
pub(super) fn transform_dml(
    query: ast_refined::Query,
    ctx: &TransformCtx,
) -> std::result::Result<Result<Lowered>, ast_refined::Query> {
    let ast_refined::Query { cfes: (), ctes, body } = query;
    match take_mutation(body, ctx) {
        Ok(mutation) => Ok((|| {
            let sql_ctes: Vec<Cte> = ctes
                .into_iter()
                .map(|binding| relational::lower_cte_binding(binding, &ctx.names, ctx))
                .collect::<Result<_>>()?;
            let mut lowered = lower_mutation(mutation, &ctx.names, ctx)?;
            // The definitions belong to whichever statements READ the
            // relations they define. A staged mutation reads its source
            // once, while it is being staged; everything after that reads
            // the staged relation and needs no definition at all.
            for statement in &mut lowered.prepare {
                merge_ctes_into_statement(statement, sql_ctes.clone());
            }
            for obligation in &mut lowered.obligations {
                merge_ctes_into_statement(&mut obligation.statement, sql_ctes.clone());
            }
            merge_ctes_into_statement(&mut lowered.statement, sql_ctes);
            Ok(lowered)
        })()),
        Err(body) => Err(ast_refined::Query { cfes: (), ctes, body }),
    }
}

/// A mutation terminal taken apart: the call that is the terminal, its
/// receipt, and the restriction/bound steps folded above it.
struct MutationTerminal {
    call: ast_refined::SealedCall,
    receipt: Option<ast_refined::Access>,
    trailing: Vec<ast_refined::Continuation>,
}

/// The one mutation partition: a chain headed by a mutation call comes back
/// as its parts; anything else comes back UNCHANGED as the Err. The head is
/// matched and moved ONCE — the successful arm constructs the terminal, and
/// every other owned head is restored into the returned chain — so there is
/// no boolean witness beside the match and no impossible arm.
fn take_mutation(
    chain: ast_refined::Chain,
    ctx: &TransformCtx,
) -> std::result::Result<MutationTerminal, ast_refined::Chain> {
    let mut chain = chain;
    let mut popped = Vec::new();
    // A restriction or bound standing after the terminal constrains the rows
    // the mutation touches; the RECEIPT is the access on what the mutation
    // publishes. Both stand above the terminal, so they come off first.
    while matches!(
        chain.continuations.last(),
        Some(
            ast_refined::Continuation::Restrict { .. }
                | ast_refined::Continuation::Bound { .. }
                | ast_refined::Continuation::Access { .. }
        )
    ) {
        popped.push(
            chain
                .continuations
                .pop()
                .expect("the loop just matched a step"),
        );
    }
    let restore = |head, mut continuations: Vec<ast_refined::Continuation>, popped: Vec<_>| {
        continuations.extend(popped.into_iter().rev());
        ast_refined::Chain {
            head,
            continuations,
        }
    };
    let ast_refined::Chain {
        head,
        continuations,
    } = chain;
    if !continuations.is_empty() {
        return Err(restore(head, continuations, popped));
    }
    match head {
        ast_refined::Grelex::Reference(ast_refined::Relation::FunctorCall { call, .. })
            if super::is_mutation_call(&call, ctx) =>
        {
            let mut receipt = None;
            let mut trailing = Vec::new();
            for step in popped {
                match step {
                    ast_refined::Continuation::Access { access, .. } => receipt = Some(access),
                    step => trailing.push(step),
                }
            }
            Ok(MutationTerminal {
                call,
                receipt,
                trailing,
            })
        }
        head => Err(restore(head, continuations, popped)),
    }
}

// ---------------------------------------------------------------------------
// The one road
// ---------------------------------------------------------------------------

/// Lower a mutation terminal: the call, its receipt, and the shaping steps
/// that stood above it.
fn lower_mutation(
    mutation: MutationTerminal,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Lowered> {
    let MutationTerminal {
        call,
        receipt,
        trailing,
    } = mutation;
    // THE SOURCE IS THE CALL'S OWN SECOND RELATION FORMAL — the descriptor's
    // layout puts the destination first and the relation being read after it.
    // A restriction or bound taken off above the terminal constrains the rows
    // the mutation touches, and the relation those rows come from is its
    // source: it moves there whole.
    let mut source = call
        .call()
        .relations()
        .nth(1)
        .cloned()
        .ok_or_else(|| DelightQLError::parse_error("DML call has no source relation"))?;
    for restriction in trailing.into_iter().rev() {
        source = source.then(restriction);
    }

    // THE ORDINARY ROAD. The same function every other consumer's operand
    // comes out of.
    let operand = descend::descend_as_query(source, names, ctx)?;

    let target_relation = call
        .call()
        .relations()
        .next()
        .cloned()
        .ok_or_else(|| DelightQLError::parse_error("DML call has no target relation"))?;
    let target_scope = relational::extract_cpr_schema(&target_relation);
    let target = ctx
        .identities
        .entity_of_scope(target_scope)
        .ok_or_else(|| DelightQLError::parse_error("DML target has no registry entity"))?;
    let callable = call.call().callee;
    // The statement names the relation and its correlated reads name it
    // again. Those are the same characters or the statement is malformed,
    // so the target's scope stops competing for a spelling here.
    ctx.identities.fix_relation_scope(target_scope, target);

    let mutation = Mutation::try_new(
        &ctx.identities,
        callable,
        operand,
        RelationTarget::Entity(target),
        target_scope,
        receipt.unwrap_or(ast_refined::Access::Unasked),
    )
    .map_err(|error| DelightQLError::parse_error(format!("DML mutation boundary: {error:?}")))?;

    refuse_bounded_mutation(&mutation, ctx)?;

    match mutation.verb() {
        crate::names::DmlVerb::Insert => build_insert(mutation, ctx).map(Lowered::from),
        crate::names::DmlVerb::Delete => build_delete(mutation, ctx).map(Lowered::from),
        crate::names::DmlVerb::Update => build_update(mutation, ctx),
    }
}

/// A relation whose rows were chosen by POSITION may not be updated or
/// deleted.
///
/// Both verbs identify their target rows by VALUE, so two rows a bound tells
/// apart are one row to the statement: matching one selected copy of a
/// duplicated row matches every equal-valued row, and the statement reports
/// success over rows the source never selected. `insert!` is exempt — it
/// writes the rows the source produces and never has to rediscover an
/// existing target row.
///
/// Asked once, for every verb, before either builder runs. Asked of the typed
/// fact the scope carries, stamped where the bound was written and inherited
/// by every scope minted above it: an alias, a wrap, a CTE binding, a set arm
/// or a join arm each offer rows the bound had a hand in choosing, and none of
/// them launders it. Nothing here reads the emitted SQL for a `LIMIT` — a
/// search whose failure is silent and whose false answer is "go ahead".
fn refuse_bounded_mutation(mutation: &Mutation<Operand>, ctx: &TransformCtx) -> Result<()> {
    let verb = match mutation.verb() {
        crate::names::DmlVerb::Insert => return Ok(()),
        crate::names::DmlVerb::Delete => "delete!",
        crate::names::DmlVerb::Update => "update!",
    };
    if !ctx
        .identities
        .is_row_bounded(mutation.source().publication().at_scope())
    {
        return Ok(());
    }
    Err(DelightQLError::validation_error_categorized(
        "dml/shape/bounded_mutation",
        format!(
            "{verb} cannot consume a relation bounded by position (`# < N`): the \
             bound chooses rows by position and the mutation reaches them by \
             value, so two rows the bound tells apart are one row to the statement"
        ),
        "restrict the source by a condition its rows carry, or read the bounded \
         rows and mutate by a key you name",
    ))
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

/// INSERT INTO target (columns) SELECT ... FROM source
///
/// The named columns are the ones the SOURCE supplies, in the order it
/// supplies them. The target's heading is what may be named, not what is: a
/// source narrower than its target is an ordinary partial insert, and naming
/// the whole target beside a shorter select list states an arity the statement
/// does not have — which the engine, not the compiler, is left to notice.
///
/// Pairing is by name, because that is how this language binds. A source
/// column the target does not have is refused here rather than silently
/// landing in whatever column shares its position.
fn build_insert(mutation: Mutation<Operand>, ctx: &TransformCtx) -> Result<SqlStatement> {
    let target_scope = mutation.target_scope();
    let target = mutation.target().clone();
    let heading = ctx.identities.known_heading(target_scope)?.to_vec();
    let supplied = mutation.source().columns().to_vec();

    let mut columns = Vec::new();
    let mut data: Vec<ColId> = Vec::new();
    for column in &supplied {
        let source = column.identity();
        // A hygienic occurrence is the compiler's own — a dispatch label, a
        // hoisted correlation carrier. It is not a value anyone wrote and it
        // stands for no column of the target. Reading the disposition the
        // registry recorded answers that; asking whether the column's scope
        // happens to have an entity answers a different question, and it
        // answered "control column" for every anonymous source, which is how
        // a misspelled header reached the engine instead of a refusal.
        if ctx.identities.addressing(source) == crate::names::Addressing::Hygienic {
            continue;
        }
        columns.push(names_one_target_column(
            source,
            &heading,
            "dml/insert/unnamed_column",
            "an inserted value names no column of its target",
            "every data column the source publishes must name one column of the \
             relation being inserted into",
            ctx,
        )?);
        data.push(source);
    }

    // Only when a control column was dropped: the statement's column list and
    // the source's row must be the same width, so the source publishes the
    // data heading through the ordinary projection every consumer uses.
    let narrowing = data.len() != supplied.len();
    let mutation = mutation.map_source(|operand| {
        if narrowing {
            operand
                .add_projection(
                    data.iter()
                        .map(|column| {
                            SelectItem::expression_with_alias(
                                DomainExpression::Column(*column),
                                *column,
                            )
                        })
                        .collect(),
                )?
                .to_sql()
        } else {
            operand.to_sql()
        }
    })?;

    Ok(SqlStatement::Insert {
        target,
        target_scope,
        columns,
        with_clause: None,
        source: mutation.into_source(),
    })
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

/// DELETE FROM target WHERE EXISTS (SELECT 1 FROM (<source>) AS _del WHERE target.c IS NOT DISTINCT FROM _del.c ...)
fn build_delete(mutation: Mutation<Operand>, ctx: &TransformCtx) -> Result<SqlStatement> {
    let target_scope = mutation.target_scope();
    let target = mutation.target().clone();
    let columns = ctx.identities.known_heading(target_scope)?.to_vec();
    let source_columns = mutation.source().columns().to_vec();
    let source = mutation.into_source().to_sql()?;
    let where_clause = build_exists_match(
        target_scope,
        &columns,
        source_columns,
        source,
        "dml/shape/delete_column_identity",
        "delete!",
        ctx,
    )?;
    Ok(SqlStatement::Delete {
        target,
        target_scope,
        with_clause: None,
        where_clause,
    })
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

/// UPDATE target SET col = (SELECT src.col FROM src WHERE <match>) WHERE EXISTS (SELECT 1 FROM src WHERE <match>)
///
/// The source is named once, as a CTE, and read from twice: once to say WHICH
/// rows change and once to say what they become. Both readings are of the
/// whole relation, which is the point — an UPDATE carries no FROM, so a road
/// that copied the source's WHERE into the statement could only carry the
/// restrictions that happened to BE a WHERE, and silently dropped every other
/// kind. A join in the FROM tree and a filter one layer down inside a name
/// each decided which rows were meant, and each vanished. Reading the source
/// whole cannot lose one, because there is nothing to select from.
///
/// A positional bound is the restriction this road does NOT carry, and it
/// never arrives here: it is refused at the verb dispatch, because what the
/// staged source cannot preserve is not the bound but the distinction between
/// two equal rows that the bound was relying on.
fn build_update(mutation: Mutation<Operand>, ctx: &TransformCtx) -> Result<Lowered> {
    let target_scope = mutation.target_scope();
    let target = mutation.target().clone();
    let heading = ctx.identities.known_heading(target_scope)?.to_vec();
    let source_columns = mutation.source().columns().to_vec();
    // THE SOURCE IS STAGED, ONCE. Everything below reads the staged
    // relation: the check that each target row is described once, and the
    // mutation that acts on it. A NAME for the source would not have been
    // enough — a name is a definition, and two statements reading one
    // definition evaluate it twice. Two evaluations are two relations
    // whenever the source is volatile, reads outside this engine, or is
    // written concurrently, and then the check has established something
    // about rows the mutation never saw.
    let mut source_query = mutation.into_source().to_sql()?;
    let staged_scope = ctx.identities.mint_derived_scope(
        crate::names::ScopeOrigin::Scratch {
            role: crate::names::ScratchRole::Snapshot,
        },
        crate::names::Hint::Prefix("dml_source"),
    );
    let published = super::builder::republish_under(
        &mut source_query,
        staged_scope,
        &source_columns,
        &ctx.identities,
        crate::names::Republish::BoundaryExport,
    )?;
    let prepare = vec![
        // A run that ended early left this behind, and the next run of the
        // same statement asks for the same name.
        SqlStatement::DropTempTable {
            table: staged_scope,
        },
        SqlStatement::CreateTempTable {
            table: staged_scope,
            with_clause: None,
            query: source_query,
        },
    ];

    let (assignments, match_columns) = classify_update_heading(&published, &heading, ctx)?;

    // Which target row a source row IS. The occurrences are paired by value
    // identity, never by spelling: a computed value that borrowed a column's
    // name is a different column, and matching on the name would choose the
    // rows to change by comparing the target against something it never
    // carried.
    let pairs = pair_by_identity(
        &match_columns,
        &published,
        "dml/shape/update_column_identity",
        "update!",
        ctx,
    )?;
    let matched = DomainExpression::and(
        pairs
            .iter()
            .map(|(target_column, source)| {
                DomainExpression::Column(*target_column)
                    .is_not_distinct_from(DomainExpression::Column(*source))
            })
            .collect(),
    );

    let where_clause = Some(DomainExpression::exists(read_source(
        staged_scope,
        DomainExpression::literal(crate::pipeline::ast_refined::LiteralValue::Number(
            "1".to_string(),
        )),
        None,
        matched.clone(),
        ctx,
    )?));

    let set_clause = assignments
        .iter()
        .map(|(target_column, written)| {
            Ok((
                *target_column,
                DomainExpression::Subquery(Box::new(read_source(
                    staged_scope,
                    DomainExpression::Column(*written),
                    Some(*written),
                    matched.clone(),
                    ctx,
                )?)),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    // WHAT THE STATEMENT CANNOT RUN WITHOUT. An update writes one value per
    // row, so a source offering two tuples for one target row does not say
    // what that row becomes — and no reading of the two is the right one,
    // including "they happen to agree": agreement of values is not evidence
    // of one tuple. Whether it holds is a fact about the DATA, so it is
    // established by reading the source before anything is written, in
    // ordinary SQL every target can answer, rather than by leaving each
    // engine to decide what a two-row scalar subquery means.
    let obligation = single_valued_obligation(staged_scope, &pairs, ctx)?;

    Ok(Lowered {
        statement: SqlStatement::Update {
            target,
            target_scope,
            with_clause: None,
            set_clause,
            where_clause,
        },
        obligations: vec![obligation],
        prepare,
        staged: vec![staged_scope],
    })
}

/// The read that establishes one source row per target row.
///
/// `SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM <staged> GROUP BY <identity>
/// HAVING count(*) > 1)` — one row when the source describes each target row
/// once, none when it does not, which is the verdict shape both roads refuse
/// on. It reads the STAGED relation, which is the rows the mutation will
/// read: a check over a second evaluation of the source would establish a
/// property of a relation nobody mutates.
///
/// The SQL is the same on every target: grouping, counting and existence are
/// what all of them agree about, unlike the cardinality of a scalar subquery.
fn single_valued_obligation(
    staged: ScopeId,
    identity: &[(ColId, ColId)],
    ctx: &TransformCtx,
) -> Result<Obligation> {
    let grouped = ctx.identities.mint_scope(
        crate::names::ScopeOrigin::AnonRelation,
        crate::names::Hint::None,
        None,
    );
    let ambiguous = super::builder::publish_at(
        grouped,
        [],
        SelectStatement::builder()
            .select(SelectItem::expression(DomainExpression::literal(
                crate::pipeline::ast_refined::LiteralValue::Number("1".to_string()),
            )))
            .from_tables(vec![TableExpression::Scope(staged)])
            .group_by(
                identity
                    .iter()
                    .map(|(_, source)| DomainExpression::Column(*source))
                    .collect(),
            )
            .having(
                DomainExpression::Function {
                    name: crate::pipeline::sql_ast::FunctionName::from("count"),
                    args: vec![DomainExpression::Star],
                    distinct: false,
                }
                .gt(DomainExpression::literal(
                    crate::pipeline::ast_refined::LiteralValue::Number("1".to_string()),
                )),
            ),
        &ctx.identities,
    )?;

    let verdict = ctx.identities.mint_scope(
        crate::names::ScopeOrigin::AnonRelation,
        crate::names::Hint::None,
        None,
    );
    let statement = super::builder::publish_at(
        verdict,
        [],
        SelectStatement::builder()
            .select(SelectItem::expression(DomainExpression::literal(
                crate::pipeline::ast_refined::LiteralValue::Number("1".to_string()),
            )))
            .where_clause(DomainExpression::not_exists(QueryExpression::Select(
                Box::new(ambiguous),
            ))),
        &ctx.identities,
    )?;
    Ok(Obligation {
        statement: SqlStatement::Query {
            with_clause: None,
            query: QueryExpression::Select(Box::new(statement)),
        },
        refusal: crate::pipeline::compiled_query::Refusal {
            identity: "dml/shape/update_ambiguous_source".to_string(),
            message: "update! refuses: the source offers more than one row for a row of \
                      the relation being updated, so the statement does not say what \
                      that row becomes. Two rows that agree are still two rows — \
                      agreement is not evidence of one row. Narrow the source so each \
                      row of the target is described once."
                .to_string(),
        },
    })
}

/// What each column the source publishes does for the mutation.
///
/// Two dispositions and a remainder, decided by what the occurrence IS
/// rather than by the shape of the SQL that produced it. A slot a cover
/// wrote is a value being written, and its published name says where. Every
/// other column either carries the target's own value — those identify the
/// rows — or belongs to something else the source happens to carry: a join
/// partner's key, a helper. That remainder is neither, and it is not an
/// error.
///
/// Returns the assignments and, with them, the target columns that still
/// identify a row: the ones nothing is writing over.
fn classify_update_heading(
    source_columns: &[ColumnMetadata],
    heading: &[ColId],
    ctx: &TransformCtx,
) -> Result<(Vec<(ColId, ColId)>, Vec<ColId>)> {
    let mut assignments: Vec<(ColId, ColId)> = Vec::new();
    for supplied in source_columns {
        let source = supplied.identity();
        if !ctx.identities.is_written_by_a_cover(source) {
            continue;
        }
        let target_column = names_one_target_column(
            source,
            heading,
            "dml/shape/update_unnamed_column",
            "a written value names no column of the relation being updated",
            "name the assignment after a column of the target: `$$(expr as column)`",
            ctx,
        )?;
        if assignments
            .iter()
            .any(|(assigned, _)| *assigned == target_column)
        {
            return Err(DelightQLError::validation_error_categorized(
                "dml/shape/update_ambiguous_assignment",
                format!(
                    "two written values name '{}', so the statement does not say \
                     which one the column becomes",
                    describe_column(target_column, ctx)
                ),
                "write each target column once in the cover",
            ));
        }
        assignments.push((target_column, source));
    }
    if assignments.is_empty() {
        return Err(DelightQLError::validation_error_categorized(
            "dml/shape/update_no_cover",
            "UPDATE requires at least one column assignment via $$(expr as col)",
            "Use $$(new_value as column_name) to specify what to change",
        ));
    }
    let match_columns: Vec<ColId> = heading
        .iter()
        .copied()
        .filter(|column| !assignments.iter().any(|(assigned, _)| assigned == column))
        .collect();
    if match_columns.is_empty() {
        return Err(DelightQLError::validation_error_categorized(
            "dml/shape/update_join_identity",
            "update! cannot identify the source's rows after its assignments: \
             every target column is being replaced",
            "leave at least one target identity column in the source so update! \
             can match each row without broadening the mutation",
        ));
    }
    Ok((assignments, match_columns))
}

/// The one target column this occurrence's published name names.
fn names_one_target_column(
    source: ColId,
    heading: &[ColId],
    category: &'static str,
    refusal: &'static str,
    remedy: &'static str,
    ctx: &TransformCtx,
) -> Result<ColId> {
    let name = ctx.identities.published_sym(source);
    let mut matching = heading
        .iter()
        .copied()
        .filter(|column| name.is_some() && ctx.identities.published_sym(*column) == name);
    match (matching.next(), matching.next()) {
        (Some(column), None) => Ok(column),
        (None, _) => Err(DelightQLError::validation_error_categorized(
            category,
            refusal.to_string(),
            remedy,
        )),
        (Some(_), Some(_)) => Err(DelightQLError::validation_error_categorized(
            category,
            format!(
                "'{}' names more than one column of its target",
                describe_column(source, ctx)
            ),
            "the relation being written publishes that name twice, so the value \
             does not say which column it is for",
        )),
    }
}

/// A SELECT over the named source, correlated to the row being updated.
///
/// `publishes` is the occurrence the statement offers, when it offers one: a
/// scalar read publishes its column, and an existence test publishes nothing
/// because the literal it selects names none.
fn read_source(
    source: ScopeId,
    item: DomainExpression,
    publishes: Option<ColId>,
    matched: DomainExpression,
    ctx: &TransformCtx,
) -> Result<QueryExpression> {
    let emitting = ctx.identities.mint_scope(
        crate::names::ScopeOrigin::AnonRelation,
        crate::names::Hint::None,
        None,
    );
    let (item, outputs) = match publishes {
        Some(column) => {
            let output = ctx.identities.republish_column(
                column,
                emitting,
                crate::names::Republish::BoundaryExport,
                ctx.identities.published(column),
                ctx.identities.addressing(column),
                |_| {},
            );
            (
                SelectItem::expression_with_alias(item, output),
                vec![output],
            )
        }
        None => (SelectItem::expression(item), Vec::new()),
    };
    let select = super::builder::publish_at(
        emitting,
        outputs,
        SelectStatement::builder()
            .select(item)
            .from_tables(vec![TableExpression::Scope(source)])
            .where_clause(matched),
        &ctx.identities,
    )?;
    Ok(QueryExpression::Select(Box::new(select)))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Whether a source occurrence IS the target's column.
///
/// A published NAME is not a proof of this, and matching on one is how a
/// computed value comes to stand for the column it borrowed the spelling of:
/// the rows that change are then chosen by comparing the target against an
/// expression it never carried.
///
/// Two proofs, and both are about the value rather than the characters. The
/// occurrences may share a republication chain — the source projected the
/// target's column forward. Or they may be two mints of ONE catalog column:
/// a DML target is minted beside the access its own source reads, so the
/// legitimate case has no chain to share and only the catalog coordinate
/// connects them. That coordinate is the physical relation and the ordinal,
/// never the entity handle, which answers "the same lookup" rather than
/// "the same table".
fn is_target_column(
    source: ColId,
    target_column: ColId,
    registry: &crate::names::Registry,
) -> bool {
    use crate::names::ColumnOrigin;
    if registry.same_value(source, target_column) {
        return true;
    }
    matches!(
        (
            registry.origin_of_col(registry.progenitor(source)),
            registry.origin_of_col(registry.progenitor(target_column)),
        ),
        (
            ColumnOrigin::CatalogColumn { entity: from, position: at },
            ColumnOrigin::CatalogColumn { entity: onto, position: slot },
        ) if registry.same_relation(from, onto) && at == slot
    )
}

/// A column named as the reader wrote it, for a refusal to quote back.
fn describe_column(column: ColId, ctx: &TransformCtx) -> String {
    let mut text = String::new();
    if let Some(spelling) = ctx.identities.published(column) {
        ctx.identities
            .write(spelling, &mut crate::names::Teaching(&mut text));
    }
    text
}

/// Each target column paired with the one occurrence of ITSELF the source
/// carries, or a refusal naming the column that is missing or doubled.
fn pair_by_identity(
    columns: &[ColId],
    source_columns: &[ColumnMetadata],
    error_category: &'static str,
    operation: &'static str,
    ctx: &TransformCtx,
) -> Result<Vec<(ColId, ColId)>> {
    columns
        .iter()
        .map(|target_column| {
            let (relation_text, action) = if operation == "delete!" {
                (
                    "relation being deleted from — the rows that die are",
                    "delete",
                )
            } else {
                ("relation being mutated — the rows it changes are", "mutate")
            };
            let matches = source_columns
                .iter()
                .map(ColumnMetadata::identity)
                .filter(|source| is_target_column(*source, *target_column, &ctx.identities))
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [source] => Ok((*target_column, *source)),
                [] => Err(DelightQLError::validation_error_categorized(
                    error_category,
                    format!(
                        "the source of this {operation} does not carry '{}', a column of \
                         the {relation_text} the \
                         ones the source still identifies, so every target column has \
                         to be there as ITSELF. A value that merely publishes the same \
                         name is a different column",
                        describe_column(*target_column, ctx)
                    ),
                    "project the target's own columns through the shaping pipes \
                     (`emp.id`, not a computed value aliased to `id`), or drop the \
                     shaping and filter instead",
                )),
                _ => Err(DelightQLError::validation_error_categorized(
                    error_category,
                    format!(
                        "the source of this {operation} carries '{}' more than once, so it \
                         does not say which occurrence identifies the rows to {action}",
                        describe_column(*target_column, ctx)
                    ),
                    "narrow the source so each of the target's columns reaches it once",
                )),
            }
        })
        .collect()
}

/// Build EXISTS subquery matching rows between target and source.
///
/// Generates:
///   EXISTS (SELECT 1 FROM (<source>) AS _del
///           WHERE target.c1 IS NOT DISTINCT FROM _del.c1 AND ...)
fn build_exists_match(
    target: ScopeId,
    columns: &[ColId],
    source_columns: Vec<ColumnMetadata>,
    mut source_query: QueryExpression,
    error_category: &'static str,
    operation: &'static str,
    ctx: &TransformCtx,
) -> Result<Option<DomainExpression>> {
    if columns.is_empty() {
        return Ok(None);
    }

    let source_scope = ctx.identities.mint_derived_scope(
        crate::names::ScopeOrigin::Wrap {
            input: ColumnMetadata::common_identity_scope(&source_columns, &ctx.identities)
                .unwrap_or(target),
            why: crate::names::WrapReason::Projection,
        },
        crate::names::Hint::None,
    );
    let active_source = super::builder::republish_under(
        &mut source_query,
        source_scope,
        &source_columns,
        &ctx.identities,
        crate::names::Republish::BoundaryExport,
    )?;

    let conditions: Vec<DomainExpression> =
        pair_by_identity(columns, &active_source, error_category, operation, ctx)?
            .into_iter()
            .map(|(target_column, source)| {
                DomainExpression::Column(target_column)
                    .is_not_distinct_from(DomainExpression::Column(source))
            })
            .collect();

    let where_expr = DomainExpression::and(conditions);

    let from_table = TableExpression::subquery(source_query, source_scope);
    let emitting_scope = ctx.identities.mint_scope(
        crate::names::ScopeOrigin::AnonRelation,
        crate::names::Hint::None,
        None,
    );

    // The match is read for existence, never for a column: the literal names
    // no occurrence and the emitting scope owns none.
    let inner_select = super::builder::publish_at(
        emitting_scope,
        [],
        SelectStatement::builder()
            .select(SelectItem::expression(DomainExpression::literal(
                crate::pipeline::ast_refined::LiteralValue::Number("1".to_string()),
            )))
            .from_tables(vec![from_table])
            .where_clause(where_expr),
        &ctx.identities,
    )?;

    let inner_query = QueryExpression::Select(Box::new(inner_select));

    Ok(Some(DomainExpression::exists(inner_query)))
}

/// Merge CTEs into an existing SqlStatement's with_clause.
fn merge_ctes_into_statement(stmt: &mut SqlStatement, ctes: Vec<Cte>) {
    if ctes.is_empty() {
        return;
    }

    let wc = match stmt {
        SqlStatement::Query { with_clause, .. }
        | SqlStatement::Delete { with_clause, .. }
        | SqlStatement::Update { with_clause, .. }
        | SqlStatement::Insert { with_clause, .. }
        | SqlStatement::CreateTempTable { with_clause, .. }
        | SqlStatement::CreateTempView { with_clause, .. } => with_clause,
        // A drop names a relation and reads none, so no definition applies.
        SqlStatement::DropTempTable { .. } => return,
    };

    match wc {
        Some(existing) => {
            let mut merged = ctes;
            merged.append(existing);
            *existing = merged;
        }
        None => {
            *wc = Some(ctes);
        }
    }
}
