// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE APPLICATION ADMISSION: one authored argument row meets one declared
//! parameter row BY POSITION, and the pairing that results is the only
//! account there is of which actual binds which formal.
//!
//! ONE SUBSTITUTION LAW and STRICT LANDING hold for every functor, pure or
//! effect: the written arguments bind a complete left prefix of the declared
//! row; the piped relation stands at its written `@` or at the place after
//! everything written; a landing at a formal that cannot hold a relation
//! refuses; and nothing searches the row for a relation-shaped formal
//! elsewhere. The build spent the landing INTO the row as a member, so the
//! row arrives whole, and this module reads it against the declaration that
//! was selected for it and answers with pairs.
//!
//! A pair is read-only and travels only inside the application that minted
//! it. A spender — the pure expansion, the effect invocation — resolves,
//! stages or refuses what its own contract permits AT EACH PAIR, and has no
//! road to a formal other than the pair's: no signature outside this module
//! takes a relation beside a row, and none answers a formal for a relation.
//! Privacy enforces that the pairing is consumed; the pairing's law is the
//! judgment written here.

use crate::diagnostic::Ho;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::operators::{HoArgument, RelationProvenance, ScalarArgument};
use crate::pipeline::asts::ddl::{HeadItems, HoParam};

/// How far an authored row must reach along the declared row.
#[derive(Clone, Copy)]
pub(in crate::defuse) enum ParamRowCompletion {
    /// An ordinary application, or a residual spend: the row completes the
    /// declared suffix through this index, and a shortfall refuses.
    CompleteThrough(usize),
    /// A residual construction: the row binds a proper left prefix, and the
    /// structural residual signature owns what remains.
    ProperPrefix,
}

/// WHAT STANDS AT A FORMAL, as the row supplied it. The kind records what
/// the author wrote and, for a relation, where it came from; what a kind
/// may bind at a formal is the spender's contract to judge, never a search
/// for another formal.
pub(in crate::defuse) enum Actual<'a> {
    /// A relation, authored or landed by a pipe. A landed one is admitted
    /// only at a relation formal — that judgment is made here — and resolves
    /// in the caller's world on its own carrier; an authored one is admitted
    /// closed.
    Relation {
        relation: &'a ast_unresolved::Chain,
        provenance: RelationProvenance,
    },
    /// An explicit rule designator.
    Rule(&'a ast_unresolved::Chain),
    /// A value.
    Value(&'a ast_unresolved::DomainExpression),
    /// Consecutive ground literals consumed for ONE listed relation formal:
    /// the inline scalar lift, `pivot_by("Maths"; "Music")`, whose rows are
    /// the argument. It is the one place a formal takes several members.
    Lifted(Vec<&'a ast_unresolved::DomainExpression>),
    /// A scalar-stratum member that supplies no term — a callable that is
    /// not a lambda, an enumeration, the whole-operand star, the context
    /// marker.
    Opaque,
    /// `_`, the disregarded position.
    Skip,
}

impl<'a> Actual<'a> {
    /// The relation this actual supplies, wherever it came from.
    pub(in crate::defuse) fn relation(&self) -> Option<&'a ast_unresolved::Chain> {
        match self {
            Actual::Relation { relation, .. } => Some(relation),
            Actual::Rule(_)
            | Actual::Value(_)
            | Actual::Lifted(_)
            | Actual::Opaque
            | Actual::Skip => None,
        }
    }

    /// The explicit rule designator this actual supplies.
    pub(in crate::defuse) fn rule(&self) -> Option<&'a ast_unresolved::Chain> {
        match self {
            Actual::Rule(rule) => Some(rule),
            Actual::Relation { .. }
            | Actual::Value(_)
            | Actual::Lifted(_)
            | Actual::Opaque
            | Actual::Skip => None,
        }
    }

    /// The designator a RULE formal reads: a configured designator has its
    /// own typed member, while the unconfigured `name(*)` spelling is shared
    /// with a whole relation actual — only the receiving formal decides
    /// which role those bytes have.
    pub(in crate::defuse) fn designator(&self) -> Option<&'a ast_unresolved::Chain> {
        self.rule().or_else(|| self.relation())
    }

    /// Whether this is the relation a pipe landed.
    pub(in crate::defuse) fn is_landed(&self) -> bool {
        matches!(
            self,
            Actual::Relation {
                provenance: RelationProvenance::Landed,
                ..
            }
        )
    }

    /// The TERM a formal that reads a name or a value receives: the value
    /// itself, or the name a named relation mentions. A relation that is not
    /// a name — an anonymous table, an inner relation, a call — has no term,
    /// and neither does a lift, an opaque member or a skip. Saying so is the
    /// whole of what this position knows; inventing a spelling would put a
    /// name nobody wrote into the body, where it refuses as a missing column
    /// or captures a real one that happens to share it.
    pub(in crate::defuse) fn term(&self) -> Option<ast_unresolved::DomainExpression> {
        match self {
            Actual::Value(value) => Some((*value).clone()),
            Actual::Relation { relation, .. } => match relation.as_read_relation() {
                Some(ast_unresolved::Relation::Ground {
                    mention: ast_unresolved::GroundMention::Named { identifier, .. },
                    ..
                }) => Some(
                    ast_unresolved::DomainExpression::lvar_builder(identifier.name.to_string())
                        .build(),
                ),
                _ => None,
            },
            Actual::Rule(_) | Actual::Lifted(_) | Actual::Opaque | Actual::Skip => None,
        }
    }

    /// THE ROW IS THE VALUE, when a relation standing at a scalar formal is
    /// one row of one column: `f(t(*) & 3)` is `f(t(*), _(3))` — the lift's
    /// own equivalence — so the scalar formal after `&` is supplied by a
    /// relation, and the set-at-a-time reading is that the row IS the value.
    /// A wider or taller relation is a relation: a scalar slot that quietly
    /// took its first cell would be guessing which one the author meant.
    pub(in crate::defuse) fn lifted_scalar(&self) -> Option<ast_unresolved::DomainExpression> {
        let relation = self.relation()?;
        let ast_unresolved::GroundForm::Literal(table) = relation.head().form() else {
            return None;
        };
        if !relation.continuations().is_empty() || table.table.body.header.is_some() {
            return None;
        }
        if table.table.body.rows.len() != 1 || table.table.body.rows.first().len() != 1 {
            return None;
        }
        Some(table.table.body.rows.first().0.first().value())
    }
}

/// ONE FORMAL AND WHAT THE ROW PUT AT IT. Made only by [`Application::admit`],
/// read through the application that holds it.
pub(in crate::defuse) struct Pair<'a> {
    position: usize,
    formal: &'a HoParam,
    actual: Actual<'a>,
}

impl<'a> Pair<'a> {
    /// The formal's index in the declared row.
    pub(in crate::defuse) fn position(&self) -> usize {
        self.position
    }

    pub(in crate::defuse) fn formal(&self) -> &'a HoParam {
        self.formal
    }

    pub(in crate::defuse) fn actual(&self) -> &Actual<'a> {
        &self.actual
    }
}

/// ONE APPLICATION, ADMITTED: one pair per formal the authored row reached,
/// in declared order, judged against the selected declaration's row.
/// Private fields, no constructor but [`Self::admit`], and nothing that
/// hands back the row or the declaration to be paired again.
pub(in crate::defuse) struct Application<'a> {
    pairs: Vec<Pair<'a>>,
    supplied_through: usize,
}

/// A member of the authored row, as the pairing reads it.
enum Member<'a> {
    Relation(&'a ast_unresolved::Chain),
    Rule(&'a ast_unresolved::Chain),
    Landed(&'a ast_unresolved::Chain),
    Value(&'a ast_unresolved::DomainExpression),
    Landing,
    Skip,
    Opaque,
}

impl<'a> Member<'a> {
    /// The row's members in written order. A group that wrote nothing, or
    /// wrote only the whole-operand glob, supplies no member to match — the
    /// glob is how a demand spells "whole", not a value handed to a formal.
    fn row(arguments: &'a ast_unresolved::CallArguments) -> Vec<Member<'a>> {
        match arguments {
            ast_unresolved::CallArguments::None => Vec::new(),
            ast_unresolved::CallArguments::HigherOrder(part) => part
                .members()
                .iter()
                .map(|member| match member {
                    HoArgument::Relation(relation) => Member::Relation(relation),
                    HoArgument::Rule(rule) => Member::Rule(rule),
                    HoArgument::Landed(relation) => Member::Landed(relation),
                    HoArgument::Value(value) => Member::Value(&value.value),
                    HoArgument::Landing(_) => Member::Landing,
                    HoArgument::Skip => Member::Skip,
                })
                .collect(),
            ast_unresolved::CallArguments::Scalar(members) => {
                if matches!(
                    members.as_slice(),
                    [] | [ScalarArgument::Spread(
                        crate::pipeline::asts::core::Spread::Glob(_)
                    )]
                ) {
                    return Vec::new();
                }
                members
                    .iter()
                    .map(|member| match member {
                        ScalarArgument::Value(value) => Member::Value(&value.value),
                        // A callable's BODY is the term the callee applies.
                        ScalarArgument::Callable(ast_unresolved::Callable::Lambda(lambda)) => {
                            Member::Value(&lambda.body)
                        }
                        ScalarArgument::Callable(_)
                        | ScalarArgument::Spread(_)
                        | ScalarArgument::Star
                        | ScalarArgument::Context(_) => Member::Opaque,
                    })
                    .collect()
            }
        }
    }
}

fn is_ground_literal(value: &ast_unresolved::DomainExpression) -> bool {
    matches!(
        value,
        ast_unresolved::DomainExpression::Application(ast_unresolved::FunctionApplication::Ground(
            _
        ))
    )
}

impl<'a> Application<'a> {
    /// ADMIT ONE APPLICATION. `formals` is the declared row of the ONE
    /// definition selected for this call; `arguments` is the row as the
    /// build left it, the landed member among the others; `start_at` is the
    /// declared index the row's first member faces (a residual spend
    /// resumes after its sealed prefix).
    ///
    /// The judgments, in the order they are made:
    ///
    /// 1. a row carrying two landed relations was damaged between the build
    ///    and here, and fails closed before any formal is bound;
    /// 2. NOWHERE TO LAND — a piped call to a declaration with no relation
    ///    formal refuses without naming a position, because saying which
    ///    formal the pipe reached instead would teach toward moving an `@`
    ///    that has nowhere to move to;
    /// 3. A COMPLETE LEFT PREFIX BESIDE THE LANDING — the written arguments
    ///    bind every formal the pipe does not, so a piped row has exactly as
    ///    many members as the declaration has formals left;
    /// 4. by position: the landed member must face a relation formal, an
    ///    unspent `@` refuses, and a listed relation formal facing ground
    ///    literals takes the whole run of them as its inline rows — unless a
    ///    scalar formal follows within the completion, where the split is
    ///    ambiguous and refuses toward the `&` marker;
    /// 5. a complete application that stops short refuses, and members left
    ///    over past the declared frontier refuse — decoding spends the whole
    ///    row, and raw member count cannot establish arity while a lift may
    ///    consume several members for one formal.
    pub(in crate::defuse) fn admit(
        entity: &str,
        formals: &'a [HoParam],
        arguments: &'a ast_unresolved::CallArguments,
        start_at: usize,
        completion: ParamRowCompletion,
    ) -> Result<Self> {
        let piped = arguments.judged()?.landed().is_some();
        let members = Member::row(arguments);
        let declared = formals.len().saturating_sub(start_at);
        if piped
            && !formals.is_empty()
            && !formals
                .iter()
                .any(|formal| matches!(formal, HoParam::Relation { .. }))
        {
            return Err(nowhere_to_land(entity));
        }
        if piped && members.len() != declared {
            return Err(incomplete_prefix(
                entity,
                members.len().saturating_sub(1),
                declared,
            ));
        }
        let complete_through = match completion {
            ParamRowCompletion::CompleteThrough(through) => through,
            ParamRowCompletion::ProperPrefix => formals.len(),
        };
        let last = formals.len().saturating_sub(1);

        let mut pairs = Vec::new();
        let mut index = 0;
        let mut position = start_at;
        while index < members.len() && position < complete_through {
            let formal = &formals[position];
            let actual = match members[index] {
                Member::Landed(relation) => {
                    if !matches!(formal, HoParam::Relation { .. }) {
                        return Err(landing_at_a_scalar(
                            entity,
                            formal.name().as_str(),
                            position,
                            last,
                        ));
                    }
                    index += 1;
                    Actual::Relation {
                        relation,
                        provenance: RelationProvenance::Landed,
                    }
                }
                Member::Landing => return Err(unspent_landing(entity)),
                Member::Skip => {
                    index += 1;
                    Actual::Skip
                }
                Member::Relation(relation) => {
                    index += 1;
                    Actual::Relation {
                        relation,
                        provenance: RelationProvenance::Authored,
                    }
                }
                Member::Rule(rule) => {
                    index += 1;
                    Actual::Rule(rule)
                }
                Member::Value(value) => {
                    let listed = matches!(
                        formal,
                        HoParam::Relation {
                            cols: HeadItems::Listed(_),
                            ..
                        }
                    );
                    if listed && is_ground_literal(value) {
                        let later_scalar = match completion {
                            ParamRowCompletion::CompleteThrough(through) => formals
                                [position + 1..through]
                                .iter()
                                .any(|formal| !matches!(formal, HoParam::Relation { .. })),
                            ParamRowCompletion::ProperPrefix => false,
                        };
                        if later_scalar {
                            return Err(lifted_boundary(entity, formal.name().as_str()));
                        }
                        let run: Vec<_> = members[index..]
                            .iter()
                            .map_while(|member| match member {
                                Member::Value(value) if is_ground_literal(value) => Some(*value),
                                _ => None,
                            })
                            .collect();
                        index += run.len();
                        Actual::Lifted(run)
                    } else {
                        index += 1;
                        Actual::Value(value)
                    }
                }
                Member::Opaque => {
                    index += 1;
                    Actual::Opaque
                }
            };
            pairs.push(Pair {
                position,
                formal,
                actual,
            });
            position += 1;
        }
        let supplied_through = position;

        if matches!(completion, ParamRowCompletion::CompleteThrough(_))
            && supplied_through < complete_through
        {
            return Err(incomplete_application(
                entity,
                complete_through.saturating_sub(start_at),
                supplied_through.saturating_sub(start_at),
            ));
        }
        if index != members.len() {
            return Err(surplus_members(entity, members.len() - index));
        }
        Ok(Application {
            pairs,
            supplied_through,
        })
    }

    /// The pairs, in declared order.
    pub(in crate::defuse) fn pairs(&self) -> &[Pair<'a>] {
        &self.pairs
    }

    /// The declared frontier the row reached: the index after the last
    /// formal it supplied.
    pub(in crate::defuse) fn supplied_through(&self) -> usize {
        self.supplied_through
    }
}

fn nowhere_to_land(entity: &str) -> DelightQLError {
    DelightQLError::from(Ho::PipeLanding {
        message: format!(
            "'{entity}' has no table-value parameter to receive pipe input (all \
             parameters are scalar)"
        ),
    })
}

/// EXPLICIT ALWAYS WINS: when scalar formals FOLLOW a listed relation formal
/// that inline rows are being lifted into, the row/scalar split is genuinely
/// ambiguous and must be marked with `&` — it is never guessed, because
/// guessing would silently take exactly one row and let the rest fall to
/// the scalars. At a residual's proper-prefix frontier, complete lifted rows
/// consume the authored prefix and the following formal remains in the
/// structural residual; that boundary is already exact and needs no second
/// marker.
fn lifted_boundary(entity: &str, param: &str) -> DelightQLError {
    DelightQLError::from(Ho::LiftedBoundary {
        message: format!(
            "ambiguous lifted-relation boundary in '{entity}': inline rows for parameter \
             '{param}' are followed by scalar parameter(s), and the split cannot be \
             guessed"
        ),
    })
}

fn unspent_landing(entity: &str) -> DelightQLError {
    DelightQLError::from(Ho::PipeLanding {
        message: format!(
            "the call to '{entity}' writes @ but nothing is piped into it — @ names the \
             landing of a piped relation; supply the argument directly, or pipe a \
             relation in with |>"
        ),
    })
}

fn landing_at_a_scalar(entity: &str, param: &str, position: usize, last: usize) -> DelightQLError {
    let message = if position == last {
        format!(
            "the pipe lands at the final parameter of '{entity}', and '{param}' \
             occupies it — a relation can land only at a table parameter (T(*) \
             or T(cols)). write @ at the parameter that receives the pipe: \
             {entity}(@, …)"
        )
    } else {
        format!(
            "the pipe lands at '{param}', parameter {position} of '{entity}', and \
             '{param}' is scalar — a relation can land only at a table parameter \
             (T(*) or T(cols)). Supply the scalar and write @ at a table parameter"
        )
    };
    DelightQLError::from(Ho::PipeLanding { message })
}

/// THE AUTHORED ROW BINDS A COMPLETE LEFT PREFIX, and the pipe supplies what
/// is still required — so exactly one formal must remain for it. A row that
/// leaves two obligations has no reading, and a full row leaves the pipe
/// nowhere to go.
///
/// The same count answers an explicit `@`: naming a non-final place does not
/// excuse the other formals, it only moves which one the pipe fills.
fn incomplete_prefix(entity: &str, supplied: usize, declared: usize) -> DelightQLError {
    let remaining = declared.saturating_sub(supplied);
    let message = if supplied >= declared {
        format!(
            "'{entity}' declares {declared} parameter(s) and the call supplies \
             {supplied} beside the piped relation, so the pipe has no formal left \
             to fill"
        )
    } else {
        format!(
            "'{entity}' declares {declared} parameter(s) and the call supplies \
             {supplied}, so {remaining} remain and the pipe can fill only one"
        )
    };
    DelightQLError::from(Ho::PipeLanding { message })
}

fn incomplete_application(entity: &str, declared: usize, supplied: usize) -> DelightQLError {
    DelightQLError::from(Ho::IncompleteApplication {
        message: format!(
            "'{entity}' declares {declared} parameter(s), but this application supplies \
             only {supplied} — every parameter is required input"
        ),
    })
}

fn surplus_members(entity: &str, surplus: usize) -> DelightQLError {
    DelightQLError::from(Ho::IncompleteApplication {
        message: format!(
            "the application of '{entity}' leaves {surplus} authored parameter-row \
             member(s) without a declared parameter"
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::asts::core::{
        AnonRelation, AnonTable, ArgumentValue, Chain, GroundForm, LiteralValue,
    };
    use delightql_types::SqlIdentifier;

    fn scalar(name: &str) -> HoParam {
        HoParam::Scalar {
            name: SqlIdentifier::new(name),
            guard: None,
            callable: false,
        }
    }

    fn relation(name: &str) -> HoParam {
        HoParam::Relation {
            name: SqlIdentifier::new(name),
            cols: HeadItems::Glob,
        }
    }

    fn literal(text: &str) -> ast_unresolved::DomainExpression {
        ast_unresolved::DomainExpression::Application(ast_unresolved::FunctionApplication::Ground(
            LiteralValue::String(text.into()),
        ))
    }

    fn rows(rows: Vec<Vec<ast_unresolved::DomainExpression>>) -> Chain {
        Chain::authored(GroundForm::Literal(AnonRelation::plain(
            AnonTable::from_values(None, rows).unwrap(),
        )))
    }

    fn value(text: &str) -> HoArgument {
        HoArgument::Value(ArgumentValue::plain(literal(text)))
    }

    fn landed() -> HoArgument {
        HoArgument::Landed(rows(vec![vec![literal("1")], vec![literal("2")]]))
    }

    fn complete(
        formals: &[HoParam],
        row: Vec<HoArgument>,
    ) -> Result<Vec<(usize, String, &'static str)>> {
        let arguments = ast_unresolved::CallArguments::higher_order(row);
        let admitted = Application::admit(
            "f",
            formals,
            &arguments,
            0,
            ParamRowCompletion::CompleteThrough(formals.len()),
        )?;
        Ok(admitted
            .pairs()
            .iter()
            .map(|pair| {
                let kind = match pair.actual() {
                    Actual::Relation {
                        provenance: RelationProvenance::Landed,
                        ..
                    } => "landed",
                    Actual::Relation { .. } => "relation",
                    Actual::Rule(_) => "rule",
                    Actual::Value(_) => "value",
                    Actual::Lifted(_) => "lifted",
                    Actual::Opaque => "opaque",
                    Actual::Skip => "skip",
                };
                (pair.position(), pair.formal().name().to_string(), kind)
            })
            .collect())
    }

    fn identity(error: DelightQLError) -> String {
        error.to_string()
    }

    /// THE POSITION IS THE FORMAL. The pipe's relation faces the formal at
    /// the place the build put it, whichever kind that formal is; nothing
    /// hunts for the relation-shaped one.
    #[test]
    fn a_landing_at_a_scalar_formal_refuses_and_at_a_relation_formal_admits() {
        let scalar_then_relation = [scalar("label"), relation("T")];
        let refused = complete(&scalar_then_relation, vec![landed(), value("z")]).unwrap_err();
        assert!(
            matches!(
                refused,
                DelightQLError::Semantic(crate::diagnostic::Semantic::Resolution(
                    crate::diagnostic::Resolution::Ho(Ho::PipeLanding { .. })
                ))
            ),
            "a relation explicitly placed at scalar `label` refuses: {}",
            identity(refused)
        );

        let relation_then_scalar = [relation("T"), scalar("label")];
        let admitted = complete(&relation_then_scalar, vec![landed(), value("z")]).unwrap();
        assert_eq!(
            admitted,
            vec![
                (0, "T".to_string(), "landed"),
                (1, "label".to_string(), "value")
            ]
        );

        let default_final = complete(&scalar_then_relation, vec![value("z"), landed()]).unwrap();
        assert_eq!(
            default_final,
            vec![
                (0, "label".to_string(), "value"),
                (1, "T".to_string(), "landed")
            ]
        );
    }

    /// The same declaration, direct and piped, binds the same formals to the
    /// same members; only the provenance differs.
    #[test]
    fn direct_and_piped_spellings_pair_identically() {
        let formals = [relation("T"), scalar("label")];
        let piped = complete(&formals, vec![landed(), value("z")]).unwrap();
        let direct = complete(
            &formals,
            vec![
                HoArgument::Relation(rows(vec![vec![literal("1")]])),
                value("z"),
            ],
        )
        .unwrap();
        assert_eq!(piped[0].0, direct[0].0);
        assert_eq!(piped[0].1, direct[0].1);
        assert_eq!(piped[1], direct[1]);
        assert_eq!(piped[0].2, "landed");
        assert_eq!(direct[0].2, "relation");
    }

    /// A pipe lands between two scalars where `@` puts it, and each scalar
    /// keeps its own place.
    #[test]
    fn an_explicit_nonfinal_landing_between_scalars_keeps_every_position() {
        let formals = [scalar("a"), relation("T"), scalar("b")];
        let admitted = complete(&formals, vec![value("x"), landed(), value("y")]).unwrap();
        assert_eq!(
            admitted,
            vec![
                (0, "a".to_string(), "value"),
                (1, "T".to_string(), "landed"),
                (2, "b".to_string(), "value"),
            ]
        );
    }

    /// A piped row binds a complete left prefix beside the landing: one
    /// short leaves two obligations, one long leaves the pipe nowhere.
    #[test]
    fn a_piped_row_must_leave_exactly_one_formal_for_the_pipe() {
        let formals = [scalar("a"), relation("T"), scalar("b")];
        for row in [
            vec![value("x"), landed()],
            vec![value("x"), landed(), value("y"), value("w")],
        ] {
            let refused = complete(&formals, row).unwrap_err();
            assert!(
                matches!(
                    refused,
                    DelightQLError::Semantic(crate::diagnostic::Semantic::Resolution(
                        crate::diagnostic::Resolution::Ho(Ho::PipeLanding { .. })
                    ))
                ),
                "{}",
                identity(refused)
            );
        }
    }

    /// Nowhere to land: no relation formal at all refuses without naming
    /// a position.
    #[test]
    fn a_pipe_into_an_all_scalar_declaration_has_nowhere_to_land() {
        let formals = [scalar("label")];
        let refused = complete(&formals, vec![landed()]).unwrap_err();
        assert!(identity(refused).contains("no table-value parameter"));
    }

    /// An `@` no pipe spent is not an argument.
    #[test]
    fn an_unspent_landing_refuses() {
        let formals = [relation("T"), scalar("label")];
        let refused = complete(
            &formals,
            vec![
                HoArgument::Landing(crate::pipeline::asts::core::AtSign),
                value("z"),
            ],
        )
        .unwrap_err();
        assert!(identity(refused).contains("nothing is piped"));
    }

    /// A complete application supplies every formal and nothing more.
    #[test]
    fn a_complete_application_refuses_a_shortfall_and_a_surplus() {
        let formals = [scalar("a"), relation("T")];
        let short = complete(&formals, vec![value("x")]).unwrap_err();
        assert!(matches!(
            short,
            DelightQLError::Semantic(crate::diagnostic::Semantic::Resolution(
                crate::diagnostic::Resolution::Ho(Ho::IncompleteApplication { .. })
            ))
        ));
        let long = complete(
            &formals,
            vec![
                value("x"),
                HoArgument::Relation(rows(vec![vec![literal("1")]])),
                value("y"),
            ],
        )
        .unwrap_err();
        assert!(matches!(
            long,
            DelightQLError::Semantic(crate::diagnostic::Semantic::Resolution(
                crate::diagnostic::Resolution::Ho(Ho::IncompleteApplication { .. })
            ))
        ));
    }

    /// EXPLICIT ALWAYS WINS: inline rows followed by a scalar formal within
    /// the completion refuse toward the `&` marker, before any member is
    /// consumed, and the same row at a proper-prefix frontier lifts.
    #[test]
    fn inline_rows_before_a_scalar_formal_refuse_toward_the_marker() {
        let listed = HoParam::Relation {
            name: SqlIdentifier::new("T"),
            cols: HeadItems::Listed(vec![
                crate::pipeline::asts::core::definitions::HeadItem::plumb(SqlIdentifier::new(
                    "label",
                )),
                crate::pipeline::asts::core::definitions::HeadItem::plumb(SqlIdentifier::new(
                    "value",
                )),
            ]),
        };
        let formals = [listed, scalar("suffix")];
        let arguments =
            ast_unresolved::CallArguments::higher_order(vec![value("a"), value("1"), value("!")]);
        let refused = Application::admit(
            "add_suffix",
            &formals,
            &arguments,
            0,
            ParamRowCompletion::CompleteThrough(formals.len()),
        )
        .err()
        .expect("the split is ambiguous");
        assert!(
            matches!(
                refused,
                DelightQLError::Semantic(crate::diagnostic::Semantic::Resolution(
                    crate::diagnostic::Resolution::Ho(Ho::LiftedBoundary { .. })
                ))
            ),
            "{}",
            identity(refused)
        );
        let prefix = Application::admit(
            "add_suffix",
            &formals,
            &arguments,
            0,
            ParamRowCompletion::ProperPrefix,
        )
        .expect("a proper prefix lifts the rows");
        assert!(matches!(prefix.pairs()[0].actual(), Actual::Lifted(run) if run.len() == 3));
    }

    /// A listed relation formal facing ground literals takes the whole run
    /// as its inline rows, and the formal after the run takes the next
    /// member.
    #[test]
    fn a_listed_relation_formal_lifts_the_run_of_literals_before_it() {
        let listed = HoParam::Relation {
            name: SqlIdentifier::new("T"),
            cols: HeadItems::Listed(vec![
                crate::pipeline::asts::core::definitions::HeadItem::plumb(SqlIdentifier::new(
                    "subject",
                )),
            ]),
        };
        let formals = [listed, relation("R")];
        let admitted = complete(
            &formals,
            vec![
                value("Maths"),
                value("Music"),
                HoArgument::Relation(rows(vec![vec![literal("1")]])),
            ],
        )
        .unwrap();
        assert_eq!(
            admitted,
            vec![
                (0, "T".to_string(), "lifted"),
                (1, "R".to_string(), "relation")
            ]
        );
    }

    /// THE ROW IS THE VALUE, and only when there is one row of one column.
    #[test]
    fn only_a_single_cell_lift_answers_a_scalar_formal() {
        let one_cell = Actual::Relation {
            relation: &rows(vec![vec![literal("3")]]),
            provenance: RelationProvenance::Authored,
        };
        assert_eq!(one_cell.lifted_scalar(), Some(literal("3")));
        let two_columns = rows(vec![vec![literal("3"), literal("4")]]);
        let two_rows = rows(vec![vec![literal("3")], vec![literal("4")]]);
        for wider in [&two_columns, &two_rows] {
            let actual = Actual::Relation {
                relation: wider,
                provenance: RelationProvenance::Authored,
            };
            assert_eq!(
                actual.lifted_scalar(),
                None,
                "a relation with more than one cell is a relation"
            );
        }
        assert_eq!(Actual::Value(&literal("3")).lifted_scalar(), None);
    }

    /// A RELATION THAT IS NOT A NAME HAS NO TERM.
    #[test]
    fn a_relation_that_is_not_a_name_yields_no_term() {
        let anonymous = rows(vec![vec![literal("3")]]);
        let named = Chain::read(
            ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Named {
                    identifier: ast_unresolved::QualifiedName {
                        namespace_path: ast_unresolved::NamespacePath::empty(),
                        name: SqlIdentifier::new("users"),
                    },
                    alias: None,
                    mutation_target: false,
                    passthrough: false,
                },
                outer: false,
            },
            ast_unresolved::Access::All,
        );
        let named_term = Actual::Relation {
            relation: &named,
            provenance: RelationProvenance::Authored,
        }
        .term();
        assert!(
            matches!(
                &named_term,
                Some(ast_unresolved::DomainExpression::Reference(
                    crate::pipeline::asts::core::Reference::Named(
                        crate::pipeline::asts::core::NamedReference(column)
                    )
                )) if column.name.as_str() == "users"
            ),
            "a named relation IS its name: {named_term:?}"
        );
        assert!(
            Actual::Relation {
                relation: &anonymous,
                provenance: RelationProvenance::Authored,
            }
            .term()
            .is_none(),
            "an anonymous relation names nothing a formal can bind"
        );
    }
}
