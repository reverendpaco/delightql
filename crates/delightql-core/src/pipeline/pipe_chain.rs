//! Pipe chain linearization utilities.
//!
//! Converts nested `Pipe(C, Pipe(B, A))` linked lists into flat `(A, [B, C])`
//! vectors for iterative processing. Eliminates pipe-spine recursion in every
//! phase that adopts it.

use crate::pipeline::asts::core::expressions::relational::RelationalExpression;
use crate::pipeline::asts::core::operators::UnaryRelationalOperator;
use crate::pipeline::asts::core::phase_box::PhaseBox;
use crate::pipeline::asts::core::CprSchema;

/// A single pipe segment: the operator applied and the schema it produces.
pub struct PipeSegment<Phase> {
    pub operator: UnaryRelationalOperator<Phase>,
    pub cpr_schema: PhaseBox<CprSchema, Phase>,
}

/// Collect `Pipe(C, Pipe(B, A))` into `(A, [B, C])`.
///
/// The returned segments are in source-code order (left-to-right): `[B, C]`.
/// The base expression `A` is the non-Pipe root.
#[stacksafe::stacksafe]
pub fn collect_pipe_chain<Phase>(
    expr: RelationalExpression<Phase>,
) -> (RelationalExpression<Phase>, Vec<PipeSegment<Phase>>) {
    let mut segments = Vec::new();
    let mut current = expr;
    while let RelationalExpression::Pipe(pipe) = current {
        let pipe = (*pipe).into_inner();
        segments.push(PipeSegment {
            operator: pipe.operator,
            cpr_schema: pipe.cpr_schema,
        });
        current = pipe.source;
    }
    segments.reverse();
    (current, segments)
}

/// Reconstruct `(A, [B, C])` back into `Pipe(C, Pipe(B, A))`.
pub fn reconstruct_pipe_chain<Phase>(
    base: RelationalExpression<Phase>,
    segments: Vec<PipeSegment<Phase>>,
) -> RelationalExpression<Phase> {
    use crate::pipeline::asts::core::expressions::pipes::PipeExpression;
    segments.into_iter().fold(base, |source, seg| {
        RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
            source,
            operator: seg.operator,
            cpr_schema: seg.cpr_schema,
        })))
    })
}
