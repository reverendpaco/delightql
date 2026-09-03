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
use crate::pipeline::sql_ast::{
    Cte, DomainExpression, QueryExpression, SelectItem, SelectStatement, SqlStatement,
    TableExpression,
};

use super::builder::{Builder, NameGenerator, Projected, Qualify};
use super::relational;
use super::{descend, Lowered, Mutation, Obligation, TransformCtx};

/// The operand a mutation consumes: one semantic relation and the SQL builder
/// lowered from that same tree.
struct Operand {
    relation: crate::relation::SemanticRelation,
    builder: Builder<Projected>,
}

impl Operand {
    fn lower(
        source: ast_refined::Chain,
        names: &NameGenerator,
        ctx: &TransformCtx,
    ) -> Result<Self> {
        let relation = source.semantic_relation();
        let builder = descend::descend_as_query(source, names, ctx)?;
        Ok(Operand { relation, builder })
    }

    fn into_builder(self) -> Builder<Projected> {
        self.builder
    }
}

impl std::ops::Deref for Operand {
    type Target = Builder<Projected>;

    fn deref(&self) -> &Self::Target {
        &self.builder
    }
}

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
    let ast_refined::Query { locals, body } = query;
    let ctes = locals.into_ctes();
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
        Err(body) => Err(ast_refined::Query::binding(
            crate::pipeline::asts::core::QueryLocals::spent(ctes),
            body,
        )),
    }
}

/// A mutation terminal taken apart: the call that is the terminal, its
/// receipt, and the restriction/bound steps folded above it.
struct MutationTerminal {
    call: ast_refined::SealedCall,
    stage: crate::relation::SemanticRelation,
    receipt: Option<ast_refined::Access>,
    /// The shaping continuations that stood above the terminal, outermost
    /// first. Carried as the TRANSPARENT forms they are, not as steps: they
    /// land on the call's source relation rather than the chain they came
    /// off, and only a form that publishes its operand's own relation may
    /// make that move.
    trailing: Vec<ast_refined::Transparent>,
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
    // The Err arm hands back exactly what came in, so the chain is kept
    // rather than reassembled from parts a caller could reorder.
    let original = chain.clone();
    let mut chain = chain;
    let mut popped = Vec::new();
    // A restriction or bound standing after the terminal constrains the rows
    // the mutation touches; the RECEIPT is the access on what the mutation
    // publishes. Both stand above the terminal, so they come off first.
    while matches!(
        chain.continuations().last().map(|step| step.form()),
        Some(
            ast_refined::Continuation::Restrict { .. }
                | ast_refined::Continuation::Bound { .. }
                | ast_refined::Continuation::Access { .. }
        )
    ) {
        popped.push(
            chain
                .pop_continuation()
                .expect("the loop just matched a step"),
        );
    }
    let (head, continuations) = chain.into_parts();
    let is_mutation = match head.form() {
        ast_refined::GroundForm::Reference(ast_refined::Relation::FunctorCall { call, .. }) => {
            super::is_mutation_call(call, ctx)
        }
        _ => false,
    };
    if !continuations.is_empty() || !is_mutation {
        return Err(original);
    }
    let stage = *head.result();
    match head.into_form() {
        ast_refined::GroundForm::Reference(ast_refined::Relation::FunctorCall { call, .. }) => {
            let mut receipt = None;
            let mut trailing = Vec::new();
            for step in popped {
                match ast_refined::Transparent::of(step.into_form()) {
                    Ok(shaping) => trailing.push(shaping),
                    Err(ast_refined::Continuation::Access { access, .. }) => receipt = Some(access),
                    Err(_) => unreachable!("the loop matched only these three forms"),
                }
            }
            Ok(MutationTerminal {
                call,
                stage,
                receipt,
                trailing,
            })
        }
        _ => unreachable!("the head was just matched as a mutation call"),
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
        stage,
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
        source = source.transparently(restriction);
    }

    // THE ORDINARY ROAD. The same function every other consumer's operand
    // comes out of.
    let operand = Operand::lower(source, names, ctx)?;

    let target_relation = call
        .call()
        .relations()
        .next()
        .cloned()
        .ok_or_else(|| DelightQLError::parse_error("DML call has no target relation"))?;
    let target_semantic = target_relation.semantic_relation();
    let target_scope = target_semantic.scope();
    let target = ctx
        .relations
        .entity(&target_semantic)?
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
        target_semantic,
        stage,
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
    if !ctx.relations.is_row_bounded(&mutation.source().relation)? {
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
    let target_relation = mutation.target_relation();
    let target = mutation.target().clone();
    let heading: Vec<_> = ctx
        .relations
        .interface(&target_relation)?
        .ports()
        .iter()
        .map(|port| port.column())
        .collect();
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
        let operand = operand.into_builder();
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
    let target_relation = mutation.target_relation();
    let target = mutation.target().clone();
    let columns: Vec<_> = ctx
        .relations
        .interface(&target_relation)?
        .ports()
        .iter()
        .map(|port| port.column())
        .collect();
    let pairs = pair_target_ports(
        &target_relation,
        &columns,
        &mutation.source().relation,
        &mutation.source().builder,
        "dml/shape/delete_column_identity",
        "delete!",
        ctx,
    )?;
    let source_columns = mutation.source().columns().to_vec();
    let source = mutation.into_source().into_builder().to_sql()?;
    let where_clause = build_exists_match(target_scope, pairs, source_columns, source, ctx)?;
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
    let target_relation = mutation.target_relation();
    let target = mutation.target().clone();
    let heading: Vec<_> = ctx
        .relations
        .interface(&target_relation)?
        .ports()
        .iter()
        .map(|port| port.column())
        .collect();
    let source_columns = mutation.source().columns().to_vec();
    let stage = mutation.stage();
    // THE SOURCE IS STAGED, ONCE. Everything below reads the staged
    // relation: the check that each target row is described once, and the
    // mutation that acts on it. A NAME for the source would not have been
    // enough — a name is a definition, and two statements reading one
    // definition evaluate it twice. Two evaluations are two relations
    // whenever the source is volatile, reads outside this engine, or is
    // written concurrently, and then the check has established something
    // about rows the mutation never saw.
    let (assignments, match_columns) = classify_update_heading(&source_columns, &heading, ctx)?;
    let source_pairs = pair_target_ports(
        &target_relation,
        &match_columns,
        &mutation.source().relation,
        &mutation.source().builder,
        "dml/shape/update_column_identity",
        "update!",
        ctx,
    )?;
    let mut source_query = mutation.into_source().into_builder().to_sql()?;
    // THE STAGED RELATION HOLDS WHAT THE SOURCE EMITS. Its heading is
    // derived with it, from columns this road already knows, so the stored
    // interface and the created table's columns are one act. Aliasing the
    // statement afterwards is rendering, not publication.
    let (staged_scope, published) =
        super::builder::stage_holding(&mut source_query, &source_columns, stage, &ctx.identities)?;
    let prepare = vec![
        // A run that ended early left this behind, and the next run of the
        // same statement asks for the same name.
        SqlStatement::DropTempTable {
            table: staged_scope.scope(),
        },
        SqlStatement::CreateTempTable {
            table: staged_scope.scope(),
            with_clause: None,
            query: source_query,
        },
    ];

    let pairs = source_pairs
        .into_iter()
        .map(|(target, source)| {
            exact_republication(source, &source_columns, &published).map(|source| (target, source))
        })
        .collect::<Result<Vec<_>>>()?;
    let assignments = assignments
        .into_iter()
        .map(|(target, source)| {
            exact_republication(source, &source_columns, &published).map(|source| (target, source))
        })
        .collect::<Result<Vec<_>>>()?;
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
        staged_scope.scope(),
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
                    staged_scope.scope(),
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
    let obligation = single_valued_obligation(staged_scope.scope(), &pairs, ctx)?;

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
    let grouped = ctx.identities.anonymous_scope(None);
    let ambiguous = (SelectStatement::builder()
        .select(SelectItem::scaffolding_value(
            DomainExpression::literal(crate::pipeline::ast_refined::LiteralValue::Number(
                "1".to_string(),
            )),
            ctx.identities.scaffolding_slot(),
        ))
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
        ))
    .standing_at(grouped)
    .map_err(crate::error::DelightQLError::parse_error)?;

    let verdict = ctx.identities.anonymous_scope(None);
    let statement = (SelectStatement::builder()
        .select(SelectItem::scaffolding_value(
            DomainExpression::literal(crate::pipeline::ast_refined::LiteralValue::Number(
                "1".to_string(),
            )),
            ctx.identities.scaffolding_slot(),
        ))
        .where_clause(DomainExpression::not_exists(QueryExpression::Select(
            Box::new(ambiguous),
        ))))
    .standing_at(verdict)
    .map_err(crate::error::DelightQLError::parse_error)?;
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
    let emitting = ctx.identities.anonymous_scope(None);
    let item = match publishes {
        Some(column) => {
            let output = ctx.identities.rebind_sql_column(
                column,
                emitting,
                ctx.identities.published(column),
            );
            SelectItem::expression_with_alias(item, output)
        }
        None => SelectItem::scaffolding_value(item, ctx.identities.scaffolding_slot()),
    };
    let select = (SelectStatement::builder()
        .select(item)
        .from_tables(vec![TableExpression::Scope(source)])
        .where_clause(matched))
    .standing_at(emitting)
    .map_err(crate::error::DelightQLError::parse_error)?;
    Ok(QueryExpression::Select(Box::new(select)))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
fn pair_target_ports(
    target_relation: &crate::relation::SemanticRelation,
    columns: &[ColId],
    source_relation: &crate::relation::SemanticRelation,
    source: &dyn Qualify,
    error_category: &'static str,
    operation: &'static str,
    ctx: &TransformCtx,
) -> Result<Vec<(ColId, ColId)>> {
    let target_storage = ctx.relations.storage(target_relation)?.ok_or_else(|| {
        DelightQLError::parse_error("a DML target has no semantic storage identity")
    })?;
    let target_ports = ctx.relations.interface(target_relation)?;
    let mut pending = vec![*source_relation];
    let mut visited = std::collections::HashSet::new();
    let mut occurrences = Vec::new();
    while let Some(relation) = pending.pop() {
        if !visited.insert(relation.relation()) {
            continue;
        }
        let storage = ctx.relations.storage(&relation)?;
        let inputs = ctx.relations.inputs(&relation)?;
        if storage == Some(target_storage) {
            occurrences.push(relation);
        }
        pending.extend(inputs);
    }
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
            let position = target_ports
                .ports()
                .iter()
                .position(|port| port.column() == *target_column)
                .ok_or_else(|| {
                    DelightQLError::parse_error(
                        "a DML target column is absent from its semantic interface",
                    )
                })?;
            let mut matches = occurrences
                .iter()
                .filter_map(|occurrence| {
                    let port = ctx
                        .relations
                        .interface(occurrence)
                        .ok()?
                        .ports()
                        .get(position)
                        .copied()?;
                    let translated = ctx
                        .relations
                        .translated_port(source_relation, port)
                        .ok()??;
                    source.rebind_port(translated).ok()
                })
                .collect::<Vec<_>>();
            matches.sort_unstable();
            matches.dedup();
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

fn exact_republication(
    source: ColId,
    before: &[ColumnMetadata],
    after: &[ColumnMetadata],
) -> Result<ColId> {
    let mut positions = before
        .iter()
        .enumerate()
        .filter_map(|(position, column)| (column.identity() == source).then_some(position));
    match (positions.next(), positions.next()) {
        (Some(position), None) => after
            .get(position)
            .map(ColumnMetadata::identity)
            .ok_or_else(|| {
                DelightQLError::parse_error("a DML republication dropped a source slot")
            }),
        (None, _) => Err(DelightQLError::parse_error(
            "a DML republication does not carry its exact source slot",
        )),
        (Some(_), Some(_)) => Err(DelightQLError::parse_error(
            "a DML source occurrence occupies more than one slot",
        )),
    }
}

/// Build EXISTS subquery matching rows between target and source.
///
/// Generates:
///   EXISTS (SELECT 1 FROM (<source>) AS _del
///           WHERE target.c1 IS NOT DISTINCT FROM _del.c1 AND ...)
fn build_exists_match(
    target: ScopeId,
    pairs: Vec<(ColId, ColId)>,
    source_columns: Vec<ColumnMetadata>,
    mut source_query: QueryExpression,
    ctx: &TransformCtx,
) -> Result<Option<DomainExpression>> {
    if pairs.is_empty() {
        return Ok(None);
    }

    let source_scope = ctx.identities.wrap_scope(
        ColumnMetadata::common_identity_scope(&source_columns, &ctx.identities).unwrap_or(target),
        crate::names::WrapReason::Projection,
    );
    let active_source = super::builder::republish_under(
        &mut source_query,
        source_scope,
        &source_columns,
        &ctx.identities,
    )?;

    let conditions: Vec<DomainExpression> = pairs
        .into_iter()
        .map(|(target, source)| {
            exact_republication(source, &source_columns, &active_source)
                .map(|source| (target, source))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|(target_column, source)| {
            DomainExpression::Column(target_column)
                .is_not_distinct_from(DomainExpression::Column(source))
        })
        .collect();

    let where_expr = DomainExpression::and(conditions);

    let from_table = TableExpression::subquery(source_query, source_scope);
    let emitting_scope = ctx.identities.anonymous_scope(None);

    // The match is read for existence, never for a column: the literal names
    // no occurrence and the emitting scope owns none.
    let inner_select = (SelectStatement::builder()
        .select(SelectItem::scaffolding_value(
            DomainExpression::literal(crate::pipeline::ast_refined::LiteralValue::Number(
                "1".to_string(),
            )),
            ctx.identities.scaffolding_slot(),
        ))
        .from_tables(vec![from_table])
        .where_clause(where_expr))
    .standing_at(emitting_scope)
    .map_err(crate::error::DelightQLError::parse_error)?;

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
