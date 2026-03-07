//! Generic AST fold infrastructure for phase-parameterized AST traversal.
//!
//! Provides an `AstFold<P>` trait following the `syn::fold::Fold` pattern.
//! Each method has a default implementation that calls the corresponding
//! `walk_*` free function for structural descent. Implementors override only
//! the methods they care about.
//!
//! The trait and walk functions are generic over the phase parameter `P`,
//! allowing the same fold infrastructure to be used across Unresolved,
//! Addressed, and other phases.
//!
//! This replaces ~7,700 lines of duplicated structural recursion across
//! grounding.rs, cfe_substitution/, cfe_postprocessing/, Phase From impls,
//! and helpers.rs. See `book/implementation/SKYWALKER-NEXTGEN.md` for the
//! full epoch plan.

use crate::error::Result;
use crate::pipeline::asts::core::expressions::functions::{CaseArm, StringTemplatePart};
use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
use crate::pipeline::asts::core::operators::{ColumnSelector, FrameBound, WindowFrame};
use crate::pipeline::asts::core::{
    ArrayMember, BooleanExpression, CteBinding, CurlyMember, DomainExpression, DomainSpec,
    FunctionExpression, ModuloSpec, OrderingSpec, PipeExpression, Query, Relation,
    RelationalExpression, RenameSpec, RepositionSpec, Row, SigmaCondition, UnaryRelationalOperator,
};

// =============================================================================
// Trait
// =============================================================================

/// A consuming fold over phase-parameterized AST nodes.
///
/// Every method takes ownership of a node and returns `Result<T>`.
/// Defaults call the corresponding `walk_*` function for structural descent.
/// Override a method to intercept specific node types; call `walk_*` inside
/// your override to continue recursion into children.
///
/// Generic over phase `P` (e.g. `Unresolved`, `Addressed`).
#[allow(unused_variables)]
pub trait AstFold<P> {
    // -- Primary fold methods ------------------------------------------------

    fn fold_query(&mut self, q: Query<P>) -> Result<Query<P>> {
        walk_query(self, q)
    }

    fn fold_relational(&mut self, e: RelationalExpression<P>) -> Result<RelationalExpression<P>> {
        walk_relational(self, e)
    }

    fn fold_relation(&mut self, r: Relation<P>) -> Result<Relation<P>> {
        walk_relation(self, r)
    }

    fn fold_boolean(&mut self, e: BooleanExpression<P>) -> Result<BooleanExpression<P>> {
        walk_boolean(self, e)
    }

    fn fold_domain(&mut self, e: DomainExpression<P>) -> Result<DomainExpression<P>> {
        walk_domain(self, e)
    }

    fn fold_function(&mut self, f: FunctionExpression<P>) -> Result<FunctionExpression<P>> {
        walk_function(self, f)
    }

    fn fold_operator(
        &mut self,
        o: UnaryRelationalOperator<P>,
    ) -> Result<UnaryRelationalOperator<P>> {
        walk_operator(self, o)
    }

    fn fold_sigma(&mut self, s: SigmaCondition<P>) -> Result<SigmaCondition<P>> {
        walk_sigma(self, s)
    }

    fn fold_pipe(&mut self, p: PipeExpression<P>) -> Result<PipeExpression<P>> {
        walk_pipe(self, p)
    }

    fn fold_inner_relation(
        &mut self,
        i: InnerRelationPattern<P>,
    ) -> Result<InnerRelationPattern<P>> {
        walk_inner_relation(self, i)
    }

    // -- Supporting fold methods ---------------------------------------------

    fn fold_domain_spec(&mut self, d: DomainSpec<P>) -> Result<DomainSpec<P>> {
        walk_domain_spec(self, d)
    }

    fn fold_cte_binding(&mut self, c: CteBinding<P>) -> Result<CteBinding<P>> {
        walk_cte_binding(self, c)
    }

    fn fold_curly_member(&mut self, m: CurlyMember<P>) -> Result<CurlyMember<P>> {
        walk_curly_member(self, m)
    }

    fn fold_case_arm(&mut self, a: CaseArm<P>) -> Result<CaseArm<P>> {
        walk_case_arm(self, a)
    }

    fn fold_string_template_part(
        &mut self,
        p: StringTemplatePart<P>,
    ) -> Result<StringTemplatePart<P>> {
        walk_string_template_part(self, p)
    }

    fn fold_array_member(&mut self, m: ArrayMember<P>) -> Result<ArrayMember<P>> {
        walk_array_member(self, m)
    }

    fn fold_ordering_spec(&mut self, o: OrderingSpec<P>) -> Result<OrderingSpec<P>> {
        walk_ordering_spec(self, o)
    }

    fn fold_window_frame(&mut self, f: WindowFrame<P>) -> Result<WindowFrame<P>> {
        walk_window_frame(self, f)
    }

    fn fold_modulo_spec(&mut self, m: ModuloSpec<P>) -> Result<ModuloSpec<P>> {
        walk_modulo_spec(self, m)
    }

    fn fold_rename_spec(&mut self, r: RenameSpec<P>) -> Result<RenameSpec<P>> {
        walk_rename_spec(self, r)
    }

    fn fold_reposition_spec(&mut self, r: RepositionSpec<P>) -> Result<RepositionSpec<P>> {
        walk_reposition_spec(self, r)
    }

    fn fold_row(&mut self, r: Row<P>) -> Result<Row<P>> {
        walk_row(self, r)
    }

    fn fold_column_selector(&mut self, c: ColumnSelector<P>) -> Result<ColumnSelector<P>> {
        walk_column_selector(self, c)
    }

    fn fold_frame_bound(&mut self, b: FrameBound<P>) -> Result<FrameBound<P>> {
        walk_frame_bound(self, b)
    }
}

// =============================================================================
// Walk functions — leaf containers
// =============================================================================

pub fn walk_domain_spec<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    spec: DomainSpec<P>,
) -> Result<DomainSpec<P>> {
    match spec {
        DomainSpec::Glob => Ok(DomainSpec::Glob),
        DomainSpec::GlobWithUsing(cols) => Ok(DomainSpec::GlobWithUsing(cols)),
        DomainSpec::GlobWithUsingAll => Ok(DomainSpec::GlobWithUsingAll),
        DomainSpec::Positional(exprs) => {
            let folded = exprs
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?;
            Ok(DomainSpec::Positional(folded))
        }
        DomainSpec::Bare => Ok(DomainSpec::Bare),
    }
}

pub fn walk_ordering_spec<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    spec: OrderingSpec<P>,
) -> Result<OrderingSpec<P>> {
    Ok(OrderingSpec {
        column: fold.fold_domain(spec.column)?,
        direction: spec.direction,
    })
}

pub fn walk_modulo_spec<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    spec: ModuloSpec<P>,
) -> Result<ModuloSpec<P>> {
    match spec {
        ModuloSpec::Columns(columns) => {
            let folded = columns
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?;
            Ok(ModuloSpec::Columns(folded))
        }
        ModuloSpec::GroupBy {
            reducing_by,
            reducing_on,
            arbitrary,
        } => Ok(ModuloSpec::GroupBy {
            reducing_by: reducing_by
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            reducing_on: reducing_on
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            arbitrary: arbitrary
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
        }),
    }
}

pub fn walk_rename_spec<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    spec: RenameSpec<P>,
) -> Result<RenameSpec<P>> {
    Ok(RenameSpec {
        from: fold.fold_domain(spec.from)?,
        to: spec.to,
    })
}

pub fn walk_reposition_spec<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    spec: RepositionSpec<P>,
) -> Result<RepositionSpec<P>> {
    Ok(RepositionSpec {
        column: fold.fold_domain(spec.column)?,
        position: spec.position,
    })
}

pub fn walk_row<P, F: AstFold<P> + ?Sized>(fold: &mut F, row: Row<P>) -> Result<Row<P>> {
    Ok(Row {
        values: row
            .values
            .into_iter()
            .map(|e| fold.fold_domain(e))
            .collect::<Result<Vec<_>>>()?,
    })
}

pub fn walk_column_selector<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    selector: ColumnSelector<P>,
) -> Result<ColumnSelector<P>> {
    match selector {
        ColumnSelector::Explicit(exprs) => {
            let folded = exprs
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?;
            Ok(ColumnSelector::Explicit(folded))
        }
        // Leaf variants — pass through
        other @ (ColumnSelector::Regex(_)
        | ColumnSelector::All
        | ColumnSelector::Positional { .. }
        | ColumnSelector::MultipleRegex(_)
        | ColumnSelector::Resolved { .. }) => Ok(other),
    }
}

pub fn walk_window_frame<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    frame: WindowFrame<P>,
) -> Result<WindowFrame<P>> {
    Ok(WindowFrame {
        mode: frame.mode,
        start: fold.fold_frame_bound(frame.start)?,
        end: fold.fold_frame_bound(frame.end)?,
    })
}

pub fn walk_frame_bound<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    bound: FrameBound<P>,
) -> Result<FrameBound<P>> {
    match bound {
        FrameBound::Unbounded => Ok(FrameBound::Unbounded),
        FrameBound::CurrentRow => Ok(FrameBound::CurrentRow),
        FrameBound::Preceding(expr) => {
            Ok(FrameBound::Preceding(Box::new(fold.fold_domain(*expr)?)))
        }
        FrameBound::Following(expr) => {
            Ok(FrameBound::Following(Box::new(fold.fold_domain(*expr)?)))
        }
    }
}

// =============================================================================
// Walk functions — expression containers
// =============================================================================

pub fn walk_string_template_part<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    part: StringTemplatePart<P>,
) -> Result<StringTemplatePart<P>> {
    match part {
        StringTemplatePart::Text(s) => Ok(StringTemplatePart::Text(s)),
        StringTemplatePart::Interpolation(expr) => Ok(StringTemplatePart::Interpolation(Box::new(
            fold.fold_domain(*expr)?,
        ))),
    }
}

pub fn walk_array_member<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    member: ArrayMember<P>,
) -> Result<ArrayMember<P>> {
    match member {
        ArrayMember::Index { path, alias } => Ok(ArrayMember::Index {
            path: Box::new(fold.fold_domain(*path)?),
            alias,
        }),
    }
}

pub fn walk_curly_member<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    member: CurlyMember<P>,
) -> Result<CurlyMember<P>> {
    match member {
        // Leaf variants — no recursive children
        m @ CurlyMember::Shorthand { .. } => Ok(m),
        m @ CurlyMember::Glob => Ok(m),
        m @ CurlyMember::Pattern { .. } => Ok(m),
        m @ CurlyMember::OrdinalRange { .. } => Ok(m),
        m @ CurlyMember::Placeholder => Ok(m),
        // Recursive variants
        CurlyMember::Comparison { condition } => Ok(CurlyMember::Comparison {
            condition: Box::new(fold.fold_boolean(*condition)?),
        }),
        CurlyMember::KeyValue {
            key,
            nested_reduction,
            value,
        } => Ok(CurlyMember::KeyValue {
            key,
            nested_reduction,
            value: Box::new(fold.fold_domain(*value)?),
        }),
        CurlyMember::PathLiteral { path, alias } => Ok(CurlyMember::PathLiteral {
            path: Box::new(fold.fold_domain(*path)?),
            alias,
        }),
    }
}

pub fn walk_case_arm<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    arm: CaseArm<P>,
) -> Result<CaseArm<P>> {
    match arm {
        CaseArm::Simple {
            test_expr,
            value,
            result,
        } => Ok(CaseArm::Simple {
            test_expr: Box::new(fold.fold_domain(*test_expr)?),
            value,
            result: Box::new(fold.fold_domain(*result)?),
        }),
        CaseArm::CurriedSimple { value, result } => Ok(CaseArm::CurriedSimple {
            value,
            result: Box::new(fold.fold_domain(*result)?),
        }),
        CaseArm::Searched { condition, result } => Ok(CaseArm::Searched {
            condition: Box::new(fold.fold_boolean(*condition)?),
            result: Box::new(fold.fold_domain(*result)?),
        }),
        CaseArm::Default { result } => Ok(CaseArm::Default {
            result: Box::new(fold.fold_domain(*result)?),
        }),
    }
}

// =============================================================================
// Walk functions — core expressions
// =============================================================================

pub fn walk_domain<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    expr: DomainExpression<P>,
) -> Result<DomainExpression<P>> {
    match expr {
        // Leaf variants — no recursive children
        e @ DomainExpression::Lvar { .. } => Ok(e),
        e @ DomainExpression::Literal { .. } => Ok(e),
        e @ DomainExpression::Projection(_) => Ok(e),
        DomainExpression::NonUnifiyingUnderscore => Ok(DomainExpression::NonUnifiyingUnderscore),
        e @ DomainExpression::ValuePlaceholder { .. } => Ok(e),
        e @ DomainExpression::Substitution(_) => Ok(e),
        e @ DomainExpression::ColumnOrdinal(_) => Ok(e),

        // Recursive variants
        DomainExpression::Function(f) => Ok(DomainExpression::Function(fold.fold_function(f)?)),
        DomainExpression::Predicate { expr, alias } => Ok(DomainExpression::Predicate {
            expr: Box::new(fold.fold_boolean(*expr)?),
            alias,
        }),
        DomainExpression::PipedExpression {
            value,
            transforms,
            alias,
        } => Ok(DomainExpression::PipedExpression {
            value: Box::new(fold.fold_domain(*value)?),
            transforms: transforms
                .into_iter()
                .map(|f| fold.fold_function(f))
                .collect::<Result<Vec<_>>>()?,
            alias,
        }),
        DomainExpression::Parenthesized { inner, alias } => Ok(DomainExpression::Parenthesized {
            inner: Box::new(fold.fold_domain(*inner)?),
            alias,
        }),
        DomainExpression::Tuple { elements, alias } => Ok(DomainExpression::Tuple {
            elements: elements
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            alias,
        }),
        DomainExpression::ScalarSubquery {
            identifier,
            subquery,
            alias,
        } => Ok(DomainExpression::ScalarSubquery {
            identifier,
            subquery: Box::new(fold.fold_relational(*subquery)?),
            alias,
        }),
        DomainExpression::PivotOf {
            value_column,
            pivot_key,
            pivot_values,
        } => Ok(DomainExpression::PivotOf {
            value_column: Box::new(fold.fold_domain(*value_column)?),
            pivot_key: Box::new(fold.fold_domain(*pivot_key)?),
            pivot_values,
        }),
    }
}

pub fn walk_boolean<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    expr: BooleanExpression<P>,
) -> Result<BooleanExpression<P>> {
    match expr {
        // Leaf variants
        e @ BooleanExpression::Using { .. } => Ok(e),
        e @ BooleanExpression::BooleanLiteral { .. } => Ok(e),
        e @ BooleanExpression::GlobCorrelation { .. } => Ok(e),
        e @ BooleanExpression::OrdinalGlobCorrelation { .. } => Ok(e),

        // Recursive variants
        BooleanExpression::Comparison {
            operator,
            left,
            right,
        } => Ok(BooleanExpression::Comparison {
            operator,
            left: Box::new(fold.fold_domain(*left)?),
            right: Box::new(fold.fold_domain(*right)?),
        }),
        BooleanExpression::And { left, right } => Ok(BooleanExpression::And {
            left: Box::new(fold.fold_boolean(*left)?),
            right: Box::new(fold.fold_boolean(*right)?),
        }),
        BooleanExpression::Or { left, right } => Ok(BooleanExpression::Or {
            left: Box::new(fold.fold_boolean(*left)?),
            right: Box::new(fold.fold_boolean(*right)?),
        }),
        BooleanExpression::Not { expr } => Ok(BooleanExpression::Not {
            expr: Box::new(fold.fold_boolean(*expr)?),
        }),
        BooleanExpression::InnerExists {
            exists,
            identifier,
            subquery,
            alias,
            using_columns,
        } => Ok(BooleanExpression::InnerExists {
            exists,
            identifier,
            subquery: Box::new(fold.fold_relational(*subquery)?),
            alias,
            using_columns,
        }),
        BooleanExpression::In {
            value,
            set,
            negated,
        } => Ok(BooleanExpression::In {
            value: Box::new(fold.fold_domain(*value)?),
            set: set
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            negated,
        }),
        BooleanExpression::InRelational {
            value,
            subquery,
            identifier,
            negated,
        } => Ok(BooleanExpression::InRelational {
            value: Box::new(fold.fold_domain(*value)?),
            subquery: Box::new(fold.fold_relational(*subquery)?),
            identifier,
            negated,
        }),
        BooleanExpression::Sigma { condition } => Ok(BooleanExpression::Sigma {
            condition: Box::new(fold.fold_sigma(*condition)?),
        }),
    }
}

pub fn walk_function<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    func: FunctionExpression<P>,
) -> Result<FunctionExpression<P>> {
    match func {
        FunctionExpression::Regular {
            name,
            namespace,
            arguments,
            alias,
            conditioned_on,
        } => Ok(FunctionExpression::Regular {
            name,
            namespace,
            arguments: arguments
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            alias,
            conditioned_on: conditioned_on
                .map(|c| fold.fold_boolean(*c).map(|b| Box::new(b)))
                .transpose()?,
        }),
        FunctionExpression::Curried {
            name,
            namespace,
            arguments,
            conditioned_on,
        } => Ok(FunctionExpression::Curried {
            name,
            namespace,
            arguments: arguments
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            conditioned_on: conditioned_on
                .map(|c| fold.fold_boolean(*c).map(|b| Box::new(b)))
                .transpose()?,
        }),
        FunctionExpression::HigherOrder {
            name,
            curried_arguments,
            regular_arguments,
            alias,
            conditioned_on,
        } => Ok(FunctionExpression::HigherOrder {
            name,
            curried_arguments: curried_arguments
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            regular_arguments: regular_arguments
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            alias,
            conditioned_on: conditioned_on
                .map(|c| fold.fold_boolean(*c).map(|b| Box::new(b)))
                .transpose()?,
        }),
        FunctionExpression::Bracket { arguments, alias } => Ok(FunctionExpression::Bracket {
            arguments: arguments
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            alias,
        }),
        FunctionExpression::Curly {
            members,
            inner_grouping_keys,
            cte_requirements,
            alias,
        } => Ok(FunctionExpression::Curly {
            members: members
                .into_iter()
                .map(|m| fold.fold_curly_member(m))
                .collect::<Result<Vec<_>>>()?,
            inner_grouping_keys: inner_grouping_keys
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            cte_requirements, // CteRequirements has PhaseBox fields — pass through
            alias,
        }),
        FunctionExpression::Array { members, alias } => Ok(FunctionExpression::Array {
            members: members
                .into_iter()
                .map(|m| fold.fold_array_member(m))
                .collect::<Result<Vec<_>>>()?,
            alias,
        }),
        FunctionExpression::MetadataTreeGroup {
            key_column,
            key_qualifier,
            key_schema,
            constructor,
            keys_only,
            cte_requirements,
            alias,
        } => Ok(FunctionExpression::MetadataTreeGroup {
            key_column,
            key_qualifier,
            key_schema,
            constructor: Box::new(fold.fold_function(*constructor)?),
            keys_only,
            cte_requirements, // PhaseBox — pass through
            alias,
        }),
        FunctionExpression::Lambda { body, alias } => Ok(FunctionExpression::Lambda {
            body: Box::new(fold.fold_domain(*body)?),
            alias,
        }),
        FunctionExpression::Infix {
            operator,
            left,
            right,
            alias,
        } => Ok(FunctionExpression::Infix {
            operator,
            left: Box::new(fold.fold_domain(*left)?),
            right: Box::new(fold.fold_domain(*right)?),
            alias,
        }),
        FunctionExpression::StringTemplate { parts, alias } => {
            Ok(FunctionExpression::StringTemplate {
                parts: parts
                    .into_iter()
                    .map(|p| fold.fold_string_template_part(p))
                    .collect::<Result<Vec<_>>>()?,
                alias,
            })
        }
        FunctionExpression::CaseExpression { arms, alias } => {
            Ok(FunctionExpression::CaseExpression {
                arms: arms
                    .into_iter()
                    .map(|a| fold.fold_case_arm(a))
                    .collect::<Result<Vec<_>>>()?,
                alias,
            })
        }
        FunctionExpression::Window {
            name,
            arguments,
            partition_by,
            order_by,
            frame,
            alias,
        } => Ok(FunctionExpression::Window {
            name,
            arguments: arguments
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            partition_by: partition_by
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            order_by: order_by
                .into_iter()
                .map(|o| fold.fold_ordering_spec(o))
                .collect::<Result<Vec<_>>>()?,
            frame: frame.map(|f| fold.fold_window_frame(f)).transpose()?,
            alias,
        }),
        FunctionExpression::JsonPath {
            source,
            path,
            alias,
        } => Ok(FunctionExpression::JsonPath {
            source: Box::new(fold.fold_domain(*source)?),
            path: Box::new(fold.fold_domain(*path)?),
            alias,
        }),
    }
}

// =============================================================================
// Walk functions — sigma, operator, pipe, inner_relation
// =============================================================================

pub fn walk_sigma<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    cond: SigmaCondition<P>,
) -> Result<SigmaCondition<P>> {
    match cond {
        SigmaCondition::Predicate(pred) => Ok(SigmaCondition::Predicate(fold.fold_boolean(pred)?)),
        SigmaCondition::TupleOrdinal(clause) => Ok(SigmaCondition::TupleOrdinal(clause)),
        SigmaCondition::Destructure {
            json_column,
            pattern,
            mode,
            destructured_schema,
        } => Ok(SigmaCondition::Destructure {
            json_column: Box::new(fold.fold_domain(*json_column)?),
            pattern: Box::new(fold.fold_function(*pattern)?),
            mode,
            destructured_schema, // PhaseBox — pass through
        }),
        SigmaCondition::SigmaCall {
            functor,
            arguments,
            exists,
        } => Ok(SigmaCondition::SigmaCall {
            functor,
            arguments: arguments
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            exists,
        }),
    }
}

pub fn walk_operator<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    op: UnaryRelationalOperator<P>,
) -> Result<UnaryRelationalOperator<P>> {
    match op {
        UnaryRelationalOperator::General {
            containment_semantic,
            expressions,
        } => Ok(UnaryRelationalOperator::General {
            containment_semantic,
            expressions: expressions
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
        }),
        UnaryRelationalOperator::Modulo {
            containment_semantic,
            spec,
        } => Ok(UnaryRelationalOperator::Modulo {
            containment_semantic,
            spec: fold.fold_modulo_spec(spec)?,
        }),
        UnaryRelationalOperator::TupleOrdering {
            containment_semantic,
            specs,
        } => Ok(UnaryRelationalOperator::TupleOrdering {
            containment_semantic,
            specs: specs
                .into_iter()
                .map(|s| fold.fold_ordering_spec(s))
                .collect::<Result<Vec<_>>>()?,
        }),
        UnaryRelationalOperator::MapCover {
            function,
            columns,
            containment_semantic,
            conditioned_on,
        } => Ok(UnaryRelationalOperator::MapCover {
            function: fold.fold_function(function)?,
            columns: columns
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            containment_semantic,
            conditioned_on: conditioned_on
                .map(|c| fold.fold_boolean(*c).map(|b| Box::new(b)))
                .transpose()?,
        }),
        UnaryRelationalOperator::ProjectOut {
            containment_semantic,
            expressions,
        } => Ok(UnaryRelationalOperator::ProjectOut {
            containment_semantic,
            expressions: expressions
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
        }),
        UnaryRelationalOperator::RenameCover { specs } => {
            Ok(UnaryRelationalOperator::RenameCover {
                specs: specs
                    .into_iter()
                    .map(|s| fold.fold_rename_spec(s))
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        UnaryRelationalOperator::Transform {
            transformations,
            conditioned_on,
        } => Ok(UnaryRelationalOperator::Transform {
            transformations: transformations
                .into_iter()
                .map(|(expr, alias, qual)| Ok((fold.fold_domain(expr)?, alias, qual)))
                .collect::<Result<Vec<_>>>()?,
            conditioned_on: conditioned_on
                .map(|c| fold.fold_boolean(*c).map(|b| Box::new(b)))
                .transpose()?,
        }),
        UnaryRelationalOperator::AggregatePipe { aggregations } => {
            Ok(UnaryRelationalOperator::AggregatePipe {
                aggregations: aggregations
                    .into_iter()
                    .map(|e| fold.fold_domain(e))
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        UnaryRelationalOperator::Reposition { moves } => Ok(UnaryRelationalOperator::Reposition {
            moves: moves
                .into_iter()
                .map(|m| fold.fold_reposition_spec(m))
                .collect::<Result<Vec<_>>>()?,
        }),
        UnaryRelationalOperator::EmbedMapCover {
            function,
            selector,
            alias_template,
            containment_semantic,
        } => Ok(UnaryRelationalOperator::EmbedMapCover {
            function: fold.fold_function(function)?,
            selector: fold.fold_column_selector(selector)?,
            alias_template,
            containment_semantic,
        }),
        UnaryRelationalOperator::HoViewApplication {
            function,
            arguments,
            first_parens_spec,
            domain_spec,
            namespace,
        } => Ok(UnaryRelationalOperator::HoViewApplication {
            function,
            arguments,
            first_parens_spec,
            domain_spec: fold.fold_domain_spec(domain_spec)?,
            namespace,
        }),
        UnaryRelationalOperator::DmlTerminal {
            kind,
            target,
            target_namespace,
            domain_spec,
        } => Ok(UnaryRelationalOperator::DmlTerminal {
            kind,
            target,
            target_namespace,
            domain_spec: fold.fold_domain_spec(domain_spec)?,
        }),
        // Leaf operators — no recursive children
        op @ UnaryRelationalOperator::MetaIze { .. } => Ok(op),
        op @ UnaryRelationalOperator::Witness { .. } => Ok(op),
        UnaryRelationalOperator::Qualify => Ok(UnaryRelationalOperator::Qualify),
        op @ UnaryRelationalOperator::Using { .. } => Ok(op),
        UnaryRelationalOperator::UsingAll => Ok(UnaryRelationalOperator::UsingAll),
        op @ UnaryRelationalOperator::InteriorDrillDown { .. } => Ok(op),
        op @ UnaryRelationalOperator::NarrowingDestructure { .. } => Ok(op),
        // DirectiveTerminal: fold arguments, preserve name
        UnaryRelationalOperator::DirectiveTerminal { name, arguments } => {
            Ok(UnaryRelationalOperator::DirectiveTerminal {
                name,
                arguments: arguments
                    .into_iter()
                    .map(|e| fold.fold_domain(e))
                    .collect::<Result<Vec<_>>>()?,
            })
        }
    }
}

pub fn walk_pipe<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    pipe: PipeExpression<P>,
) -> Result<PipeExpression<P>> {
    Ok(PipeExpression {
        source: fold.fold_relational(pipe.source)?,
        operator: fold.fold_operator(pipe.operator)?,
        cpr_schema: pipe.cpr_schema, // PhaseBox — pass through
    })
}

pub fn walk_inner_relation<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    pattern: InnerRelationPattern<P>,
) -> Result<InnerRelationPattern<P>> {
    match pattern {
        InnerRelationPattern::Indeterminate {
            identifier,
            subquery,
        } => Ok(InnerRelationPattern::Indeterminate {
            identifier,
            subquery: Box::new(fold.fold_relational(*subquery)?),
        }),
        InnerRelationPattern::UncorrelatedDerivedTable {
            identifier,
            subquery,
            is_consulted_view,
        } => Ok(InnerRelationPattern::UncorrelatedDerivedTable {
            identifier,
            subquery: Box::new(fold.fold_relational(*subquery)?),
            is_consulted_view,
        }),
        InnerRelationPattern::CorrelatedScalarJoin {
            identifier,
            correlation_filters,
            subquery,
            hygienic_injections,
        } => Ok(InnerRelationPattern::CorrelatedScalarJoin {
            identifier,
            correlation_filters: correlation_filters
                .into_iter()
                .map(|f| fold.fold_boolean(f))
                .collect::<Result<Vec<_>>>()?,
            subquery: Box::new(fold.fold_relational(*subquery)?),
            hygienic_injections,
        }),
        InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations,
            subquery,
            hygienic_injections,
        } => Ok(InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            correlation_filters: correlation_filters
                .into_iter()
                .map(|f| fold.fold_boolean(f))
                .collect::<Result<Vec<_>>>()?,
            aggregations: aggregations
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            subquery: Box::new(fold.fold_relational(*subquery)?),
            hygienic_injections,
        }),
        InnerRelationPattern::CorrelatedWindowJoin {
            identifier,
            correlation_filters,
            order_by,
            limit,
            subquery,
        } => Ok(InnerRelationPattern::CorrelatedWindowJoin {
            identifier,
            correlation_filters: correlation_filters
                .into_iter()
                .map(|f| fold.fold_boolean(f))
                .collect::<Result<Vec<_>>>()?,
            order_by: order_by
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            limit,
            subquery: Box::new(fold.fold_relational(*subquery)?),
        }),
    }
}

// =============================================================================
// Walk functions — relational layer
// =============================================================================

pub fn walk_relation<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    rel: Relation<P>,
) -> Result<Relation<P>> {
    match rel {
        Relation::Ground {
            identifier,
            canonical_name,
            domain_spec,
            alias,
            outer,
            mutation_target,
            passthrough,
            cpr_schema,
            hygienic_injections,
        } => Ok(Relation::Ground {
            identifier,
            canonical_name,
            domain_spec: fold.fold_domain_spec(domain_spec)?,
            alias,
            outer,
            mutation_target,
            passthrough,
            cpr_schema,
            hygienic_injections,
        }),
        Relation::Anonymous {
            column_headers,
            rows,
            alias,
            outer,
            exists_mode,
            qua_target,
            cpr_schema,
        } => Ok(Relation::Anonymous {
            column_headers: column_headers
                .map(|headers| {
                    headers
                        .into_iter()
                        .map(|h| fold.fold_domain(h))
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?,
            rows: rows
                .into_iter()
                .map(|r| fold.fold_row(r))
                .collect::<Result<Vec<_>>>()?,
            alias,
            outer,
            exists_mode,
            qua_target,
            cpr_schema,
        }),
        Relation::TVF {
            function,
            arguments,
            argument_groups,
            first_parens_spec,
            domain_spec,
            alias,
            namespace,
            grounding,
            cpr_schema,
        } => Ok(Relation::TVF {
            function,
            arguments,
            argument_groups,
            first_parens_spec,
            domain_spec: fold.fold_domain_spec(domain_spec)?,
            alias,
            namespace,
            grounding,
            cpr_schema,
        }),
        Relation::InnerRelation {
            pattern,
            alias,
            outer,
            cpr_schema,
        } => Ok(Relation::InnerRelation {
            pattern: fold.fold_inner_relation(pattern)?,
            alias,
            outer,
            cpr_schema,
        }),
        Relation::ConsultedView {
            identifier,
            body,
            scoped,
            outer,
        } => Ok(Relation::ConsultedView {
            identifier,
            body: Box::new(fold.fold_query(*body)?),
            scoped,
            outer,
        }),
        Relation::PseudoPredicate {
            name,
            arguments,
            alias,
            cpr_schema,
        } => Ok(Relation::PseudoPredicate {
            name,
            arguments: arguments
                .into_iter()
                .map(|e| fold.fold_domain(e))
                .collect::<Result<Vec<_>>>()?,
            alias,
            cpr_schema,
        }),
    }
}

#[stacksafe::stacksafe]
pub fn walk_relational<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    expr: RelationalExpression<P>,
) -> Result<RelationalExpression<P>> {
    match expr {
        RelationalExpression::Relation(rel) => {
            Ok(RelationalExpression::Relation(fold.fold_relation(rel)?))
        }
        RelationalExpression::Join {
            left,
            right,
            join_condition,
            join_type,
            cpr_schema,
        } => Ok(RelationalExpression::Join {
            left: Box::new(fold.fold_relational(*left)?),
            right: Box::new(fold.fold_relational(*right)?),
            join_condition: join_condition.map(|c| fold.fold_boolean(c)).transpose()?,
            join_type,
            cpr_schema,
        }),
        RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema,
        } => Ok(RelationalExpression::Filter {
            source: Box::new(fold.fold_relational(*source)?),
            condition: fold.fold_sigma(condition)?,
            origin,
            cpr_schema,
        }),
        RelationalExpression::Pipe(pipe) => Ok(RelationalExpression::Pipe(Box::new(
            stacksafe::StackSafe::new(fold.fold_pipe((*pipe).into_inner())?),
        ))),
        RelationalExpression::SetOperation {
            operator,
            operands,
            correlation,
            cpr_schema,
        } => Ok(RelationalExpression::SetOperation {
            operator,
            operands: operands
                .into_iter()
                .map(|e| fold.fold_relational(e))
                .collect::<Result<Vec<_>>>()?,
            correlation,
            cpr_schema,
        }),
        RelationalExpression::ErJoinChain { relations } => Ok(RelationalExpression::ErJoinChain {
            relations: relations
                .into_iter()
                .map(|r| fold.fold_relation(r))
                .collect::<Result<Vec<_>>>()?,
        }),
        RelationalExpression::ErTransitiveJoin { left, right } => {
            Ok(RelationalExpression::ErTransitiveJoin {
                left: Box::new(fold.fold_relational(*left)?),
                right: Box::new(fold.fold_relational(*right)?),
            })
        }
    }
}

// =============================================================================
// Walk functions — top-level
// =============================================================================

pub fn walk_cte_binding<P, F: AstFold<P> + ?Sized>(
    fold: &mut F,
    cte: CteBinding<P>,
) -> Result<CteBinding<P>> {
    Ok(CteBinding {
        expression: fold.fold_relational(cte.expression)?,
        name: cte.name,
        is_recursive: cte.is_recursive, // PhaseBox — pass through
    })
}

pub fn walk_query<P, F: AstFold<P> + ?Sized>(fold: &mut F, query: Query<P>) -> Result<Query<P>> {
    match query {
        Query::Relational(expr) => Ok(Query::Relational(fold.fold_relational(expr)?)),
        Query::WithCtes { ctes, query } => Ok(Query::WithCtes {
            ctes: ctes
                .into_iter()
                .map(|c| fold.fold_cte_binding(c))
                .collect::<Result<Vec<_>>>()?,
            query: fold.fold_relational(query)?,
        }),
        Query::WithCfes { cfes, query } => Ok(Query::WithCfes {
            cfes, // CfeDefinition bodies are DomainExpression<P> but owned by precompiler
            query: Box::new(fold.fold_query(*query)?),
        }),
        Query::WithPrecompiledCfes { cfes, query } => Ok(Query::WithPrecompiledCfes {
            cfes, // PrecompiledCfeDefinition has Refined-phase body — not our phase
            query: Box::new(fold.fold_query(*query)?),
        }),
        Query::ReplTempTable { query, table_name } => Ok(Query::ReplTempTable {
            query: Box::new(fold.fold_query(*query)?),
            table_name,
        }),
        Query::WithErContext { context, query } => Ok(Query::WithErContext {
            context,
            query: Box::new(fold.fold_query(*query)?),
        }),
        Query::ReplTempView { query, view_name } => Ok(Query::ReplTempView {
            query: Box::new(fold.fold_query(*query)?),
            view_name,
        }),
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::asts::core::expressions::helpers::QualifiedName;
    use crate::pipeline::asts::core::{LiteralValue, NamespacePath, PhaseBox, Unresolved};
    use delightql_types::SqlIdentifier;

    fn qname(name: &str) -> QualifiedName {
        QualifiedName {
            namespace_path: NamespacePath::empty(),
            name: SqlIdentifier::new(name),
            grounding: None,
        }
    }

    fn ground(name: &str) -> Relation<Unresolved> {
        Relation::Ground {
            identifier: qname(name),
            canonical_name: PhaseBox::phantom(),
            domain_spec: DomainSpec::Glob,
            alias: None,
            outer: false,
            mutation_target: false,
            passthrough: false,
            cpr_schema: PhaseBox::phantom(),
            hygienic_injections: vec![],
        }
    }

    /// No-op fold: all defaults. Verifies the infrastructure compiles and
    /// round-trips simple ASTs unchanged.
    struct NoOpFold;
    impl AstFold<Unresolved> for NoOpFold {}

    #[test]
    fn noop_fold_roundtrips_simple_relation() {
        let ast = Query::Relational(RelationalExpression::Relation(ground("users")));
        let original = ast.clone();
        let mut fold = NoOpFold;
        let result = fold.fold_query(ast).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn noop_fold_roundtrips_pipe_with_filter() {
        let source = RelationalExpression::Relation(ground("orders"));
        let filter_expr = RelationalExpression::Filter {
            source: Box::new(source),
            condition: SigmaCondition::Predicate(BooleanExpression::Comparison {
                operator: "=".to_string(),
                left: Box::new(DomainExpression::Lvar {
                    name: SqlIdentifier::new("status"),
                    qualifier: None,
                    namespace_path: NamespacePath::empty(),
                    alias: None,
                    provenance: PhaseBox::phantom(),
                }),
                right: Box::new(DomainExpression::Literal {
                    value: LiteralValue::String("active".to_string()),
                    alias: None,
                }),
            }),
            origin:
                crate::pipeline::asts::core::expressions::metadata_types::FilterOrigin::UserWritten,
            cpr_schema: PhaseBox::phantom(),
        };

        let ast = Query::Relational(filter_expr);
        let original = ast.clone();
        let mut fold = NoOpFold;
        let result = fold.fold_query(ast).unwrap();
        assert_eq!(result, original);
    }

    /// Override fold that counts function expressions encountered.
    struct CountFunctions {
        count: usize,
    }

    impl AstFold<Unresolved> for CountFunctions {
        fn fold_function(
            &mut self,
            f: FunctionExpression<Unresolved>,
        ) -> Result<FunctionExpression<Unresolved>> {
            self.count += 1;
            walk_function(self, f)
        }
    }

    #[test]
    fn count_functions_fold() {
        // Build: users(*) |> (sum:(total), count:(*))
        let source = RelationalExpression::Relation(ground("users"));
        let pipe = PipeExpression {
            source,
            operator: UnaryRelationalOperator::General {
                containment_semantic: crate::pipeline::asts::core::ContainmentSemantic::Parenthesis,
                expressions: vec![
                    DomainExpression::Function(FunctionExpression::Curried {
                        name: SqlIdentifier::new("sum"),
                        namespace: None,
                        arguments: vec![DomainExpression::Lvar {
                            name: SqlIdentifier::new("total"),
                            qualifier: None,
                            namespace_path: NamespacePath::empty(),
                            alias: None,
                            provenance: PhaseBox::phantom(),
                        }],
                        conditioned_on: None,
                    }),
                    DomainExpression::Function(FunctionExpression::Curried {
                        name: SqlIdentifier::new("count"),
                        namespace: None,
                        arguments: vec![DomainExpression::Projection(ProjectionExpr::Glob {
                            qualifier: None,
                            namespace_path: NamespacePath::empty(),
                        })],
                        conditioned_on: None,
                    }),
                ],
            },
            cpr_schema: PhaseBox::phantom(),
        };

        let ast = Query::Relational(RelationalExpression::Pipe(Box::new(
            stacksafe::StackSafe::new(pipe),
        )));
        let mut counter = CountFunctions { count: 0 };
        let _result = counter.fold_query(ast).unwrap();
        assert_eq!(
            counter.count, 2,
            "Should count exactly 2 function expressions"
        );
    }

    #[test]
    fn noop_fold_roundtrips_ctes() {
        let cte = CteBinding {
            expression: RelationalExpression::Relation(ground("orders")),
            name: "recent_orders".to_string(),
            is_recursive: PhaseBox::phantom(),
        };

        let main_query = RelationalExpression::Relation(ground("recent_orders"));
        let ast = Query::WithCtes {
            ctes: vec![cte],
            query: main_query,
        };
        let original = ast.clone();
        let mut fold = NoOpFold;
        let result = fold.fold_query(ast).unwrap();
        assert_eq!(result, original);
    }
}
