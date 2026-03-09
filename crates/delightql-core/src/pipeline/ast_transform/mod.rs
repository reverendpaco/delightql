//! Cross-phase AST transformation infrastructure.
//!
//! `AstTransform<P, Q>` transforms AST nodes from phase `P` to phase `Q`.
//! Walk functions handle structural descent; implementors override methods
//! to hook specific node types. Default walk uses `PhaseBox::rephase()` for
//! Q-phase metadata — preserving data while retagging the phase. Hooks
//! override to populate real data when needed.
//!
//! This is the single walk infrastructure for the entire pipeline.

use crate::error::Result;
use crate::pipeline::asts::core::expressions::functions::{CaseArm, StringTemplatePart};
use crate::pipeline::asts::core::expressions::metadata_types::CteRequirements;
use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
use crate::pipeline::asts::core::operators::{ColumnSelector, FrameBound, WindowFrame};
use crate::pipeline::asts::core::{
    ArrayMember, BooleanExpression, CteBinding, CurlyMember, DomainExpression, DomainSpec,
    FunctionExpression, ModuloSpec, OrderingSpec, PhaseBox, PipeExpression, Query, Relation,
    RelationalExpression, RenameSpec, RepositionSpec, Row, SigmaCondition, UnaryRelationalOperator,
};

// =============================================================================
// FoldAction
// =============================================================================

/// Controls whether the walk recurses into children after a hook returns.
pub enum FoldAction<T> {
    /// The hook pre-processed the node; the walk should recurse into its
    /// children as usual.
    Continue(T),
    /// The hook fully handled this subtree (e.g., ran FAR on it). The walk
    /// should use this node as-is, skipping child recursion.
    Replaced(T),
}

impl<T> FoldAction<T> {
    pub fn into_inner(self) -> T {
        match self {
            FoldAction::Continue(t) | FoldAction::Replaced(t) => t,
        }
    }

    pub fn is_replaced(&self) -> bool {
        matches!(self, FoldAction::Replaced(_))
    }
}

// =============================================================================
// Trait
// =============================================================================

/// A consuming cross-phase transformation over AST nodes.
///
/// Transforms nodes from phase `P` to phase `Q`. Every method takes ownership
/// of a P-phase node and returns `Result<Q-phase node>`. Defaults call the
/// corresponding `walk_transform_*` function which mechanically converts
/// structure using `PhaseBox::rephase()` for Q-phase metadata.
///
/// Override methods to intercept specific node types (e.g., the resolver
/// overrides `transform_relation` to populate CprSchema).
#[allow(unused_variables)]
pub trait AstTransform<P, Q> {
    // -- Primary transform methods --------------------------------------------

    fn transform_query(&mut self, q: Query<P>) -> Result<Query<Q>> {
        walk_transform_query(self, q)
    }

    fn transform_relational(
        &mut self,
        e: RelationalExpression<P>,
    ) -> Result<RelationalExpression<Q>> {
        walk_transform_relational(self, e)
    }

    fn transform_relation(&mut self, r: Relation<P>) -> Result<Relation<Q>> {
        walk_transform_relation(self, r)
    }

    fn transform_boolean(&mut self, e: BooleanExpression<P>) -> Result<BooleanExpression<Q>> {
        walk_transform_boolean(self, e)
    }

    fn transform_domain(&mut self, e: DomainExpression<P>) -> Result<DomainExpression<Q>> {
        walk_transform_domain(self, e)
    }

    fn transform_function(&mut self, f: FunctionExpression<P>) -> Result<FunctionExpression<Q>> {
        walk_transform_function(self, f)
    }

    fn transform_operator(
        &mut self,
        o: UnaryRelationalOperator<P>,
    ) -> Result<UnaryRelationalOperator<Q>> {
        walk_transform_operator(self, o)
    }

    fn transform_sigma(&mut self, s: SigmaCondition<P>) -> Result<SigmaCondition<Q>> {
        walk_transform_sigma(self, s)
    }

    fn transform_pipe(&mut self, p: PipeExpression<P>) -> Result<PipeExpression<Q>> {
        walk_transform_pipe(self, p)
    }

    fn transform_inner_relation(
        &mut self,
        i: InnerRelationPattern<P>,
    ) -> Result<InnerRelationPattern<Q>> {
        walk_transform_inner_relation(self, i)
    }

    // -- Action hooks (FoldAction wrappers) -----------------------------------
    // These wrap the non-action hooks in FoldAction::Continue by default.
    // Override to return FoldAction::Replaced when a pass fully handles a
    // subtree (e.g., the refiner's FAR cycle).

    fn transform_relational_action(
        &mut self,
        e: RelationalExpression<P>,
    ) -> Result<FoldAction<RelationalExpression<Q>>> {
        self.transform_relational(e).map(FoldAction::Continue)
    }

    fn transform_relation_action(&mut self, r: Relation<P>) -> Result<FoldAction<Relation<Q>>> {
        self.transform_relation(r).map(FoldAction::Continue)
    }

    // -- Supporting transform methods -----------------------------------------

    fn transform_domain_spec(&mut self, d: DomainSpec<P>) -> Result<DomainSpec<Q>> {
        walk_transform_domain_spec(self, d)
    }

    fn transform_cte_binding(&mut self, c: CteBinding<P>) -> Result<CteBinding<Q>> {
        walk_transform_cte_binding(self, c)
    }

    fn transform_curly_member(&mut self, m: CurlyMember<P>) -> Result<CurlyMember<Q>> {
        walk_transform_curly_member(self, m)
    }

    fn transform_case_arm(&mut self, a: CaseArm<P>) -> Result<CaseArm<Q>> {
        walk_transform_case_arm(self, a)
    }

    fn transform_string_template_part(
        &mut self,
        p: StringTemplatePart<P>,
    ) -> Result<StringTemplatePart<Q>> {
        walk_transform_string_template_part(self, p)
    }

    fn transform_array_member(&mut self, m: ArrayMember<P>) -> Result<ArrayMember<Q>> {
        walk_transform_array_member(self, m)
    }

    fn transform_ordering_spec(&mut self, o: OrderingSpec<P>) -> Result<OrderingSpec<Q>> {
        walk_transform_ordering_spec(self, o)
    }

    fn transform_window_frame(&mut self, f: WindowFrame<P>) -> Result<WindowFrame<Q>> {
        walk_transform_window_frame(self, f)
    }

    fn transform_modulo_spec(&mut self, m: ModuloSpec<P>) -> Result<ModuloSpec<Q>> {
        walk_transform_modulo_spec(self, m)
    }

    fn transform_rename_spec(&mut self, r: RenameSpec<P>) -> Result<RenameSpec<Q>> {
        walk_transform_rename_spec(self, r)
    }

    fn transform_reposition_spec(&mut self, r: RepositionSpec<P>) -> Result<RepositionSpec<Q>> {
        walk_transform_reposition_spec(self, r)
    }

    fn transform_row(&mut self, r: Row<P>) -> Result<Row<Q>> {
        walk_transform_row(self, r)
    }

    fn transform_column_selector(&mut self, c: ColumnSelector<P>) -> Result<ColumnSelector<Q>> {
        walk_transform_column_selector(self, c)
    }

    fn transform_frame_bound(&mut self, b: FrameBound<P>) -> Result<FrameBound<Q>> {
        walk_transform_frame_bound(self, b)
    }
}

// =============================================================================
// Walk functions — leaf containers
// =============================================================================

pub fn walk_transform_domain_spec<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    spec: DomainSpec<P>,
) -> Result<DomainSpec<Q>> {
    match spec {
        DomainSpec::Glob => Ok(DomainSpec::Glob),
        DomainSpec::GlobWithUsing(cols) => Ok(DomainSpec::GlobWithUsing(cols)),
        DomainSpec::GlobWithUsingAll => Ok(DomainSpec::GlobWithUsingAll),
        DomainSpec::Positional(exprs) => {
            let transformed = exprs
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?;
            Ok(DomainSpec::Positional(transformed))
        }
        DomainSpec::Bare => Ok(DomainSpec::Bare),
    }
}

pub fn walk_transform_ordering_spec<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    spec: OrderingSpec<P>,
) -> Result<OrderingSpec<Q>> {
    Ok(OrderingSpec {
        column: t.transform_domain(spec.column)?,
        direction: spec.direction,
    })
}

pub fn walk_transform_modulo_spec<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    spec: ModuloSpec<P>,
) -> Result<ModuloSpec<Q>> {
    match spec {
        ModuloSpec::Columns(columns) => {
            let transformed = columns
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?;
            Ok(ModuloSpec::Columns(transformed))
        }
        ModuloSpec::GroupBy {
            reducing_by,
            reducing_on,
            arbitrary,
        } => Ok(ModuloSpec::GroupBy {
            reducing_by: reducing_by
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            reducing_on: reducing_on
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            arbitrary: arbitrary
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
        }),
    }
}

pub fn walk_transform_rename_spec<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    spec: RenameSpec<P>,
) -> Result<RenameSpec<Q>> {
    Ok(RenameSpec {
        from: t.transform_domain(spec.from)?,
        to: spec.to,
    })
}

pub fn walk_transform_reposition_spec<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    spec: RepositionSpec<P>,
) -> Result<RepositionSpec<Q>> {
    Ok(RepositionSpec {
        column: t.transform_domain(spec.column)?,
        position: spec.position,
    })
}

pub fn walk_transform_row<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    row: Row<P>,
) -> Result<Row<Q>> {
    Ok(Row {
        values: row
            .values
            .into_iter()
            .map(|e| t.transform_domain(e))
            .collect::<Result<Vec<_>>>()?,
    })
}

pub fn walk_transform_column_selector<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    selector: ColumnSelector<P>,
) -> Result<ColumnSelector<Q>> {
    match selector {
        ColumnSelector::Explicit(exprs) => {
            let transformed = exprs
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?;
            Ok(ColumnSelector::Explicit(transformed))
        }
        ColumnSelector::Regex(r) => Ok(ColumnSelector::Regex(r)),
        ColumnSelector::All => Ok(ColumnSelector::All),
        ColumnSelector::Positional { start, end } => Ok(ColumnSelector::Positional { start, end }),
        ColumnSelector::MultipleRegex(rs) => Ok(ColumnSelector::MultipleRegex(rs)),
        ColumnSelector::Resolved {
            columns,
            original_selector,
        } => Ok(ColumnSelector::Resolved {
            columns,
            original_selector,
        }),
    }
}

pub fn walk_transform_window_frame<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    frame: WindowFrame<P>,
) -> Result<WindowFrame<Q>> {
    Ok(WindowFrame {
        mode: frame.mode,
        start: t.transform_frame_bound(frame.start)?,
        end: t.transform_frame_bound(frame.end)?,
    })
}

pub fn walk_transform_frame_bound<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    bound: FrameBound<P>,
) -> Result<FrameBound<Q>> {
    match bound {
        FrameBound::Unbounded => Ok(FrameBound::Unbounded),
        FrameBound::CurrentRow => Ok(FrameBound::CurrentRow),
        FrameBound::Preceding(expr) => {
            Ok(FrameBound::Preceding(Box::new(t.transform_domain(*expr)?)))
        }
        FrameBound::Following(expr) => {
            Ok(FrameBound::Following(Box::new(t.transform_domain(*expr)?)))
        }
    }
}

// =============================================================================
// Walk functions — expression containers
// =============================================================================

pub fn walk_transform_string_template_part<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    part: StringTemplatePart<P>,
) -> Result<StringTemplatePart<Q>> {
    match part {
        StringTemplatePart::Text(s) => Ok(StringTemplatePart::Text(s)),
        StringTemplatePart::Interpolation(expr) => Ok(StringTemplatePart::Interpolation(Box::new(
            t.transform_domain(*expr)?,
        ))),
    }
}

pub fn walk_transform_array_member<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    member: ArrayMember<P>,
) -> Result<ArrayMember<Q>> {
    match member {
        ArrayMember::Index { path, alias } => Ok(ArrayMember::Index {
            path: Box::new(t.transform_domain(*path)?),
            alias,
        }),
    }
}

pub fn walk_transform_curly_member<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    member: CurlyMember<P>,
) -> Result<CurlyMember<Q>> {
    match member {
        CurlyMember::Shorthand {
            column,
            qualifier,
            schema,
        } => Ok(CurlyMember::Shorthand {
            column,
            qualifier,
            schema,
        }),
        CurlyMember::Glob => Ok(CurlyMember::Glob),
        CurlyMember::Pattern { pattern } => Ok(CurlyMember::Pattern { pattern }),
        CurlyMember::OrdinalRange { start, end } => Ok(CurlyMember::OrdinalRange { start, end }),
        CurlyMember::Placeholder => Ok(CurlyMember::Placeholder),
        CurlyMember::Comparison { condition } => Ok(CurlyMember::Comparison {
            condition: Box::new(t.transform_boolean(*condition)?),
        }),
        CurlyMember::KeyValue {
            key,
            nested_reduction,
            value,
        } => Ok(CurlyMember::KeyValue {
            key,
            nested_reduction,
            value: Box::new(t.transform_domain(*value)?),
        }),
        CurlyMember::PathLiteral { path, alias } => Ok(CurlyMember::PathLiteral {
            path: Box::new(t.transform_domain(*path)?),
            alias,
        }),
    }
}

pub fn walk_transform_case_arm<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    arm: CaseArm<P>,
) -> Result<CaseArm<Q>> {
    match arm {
        CaseArm::Simple {
            test_expr,
            value,
            result,
        } => Ok(CaseArm::Simple {
            test_expr: Box::new(t.transform_domain(*test_expr)?),
            value,
            result: Box::new(t.transform_domain(*result)?),
        }),
        CaseArm::CurriedSimple { value, result } => Ok(CaseArm::CurriedSimple {
            value,
            result: Box::new(t.transform_domain(*result)?),
        }),
        CaseArm::Searched { condition, result } => Ok(CaseArm::Searched {
            condition: Box::new(t.transform_boolean(*condition)?),
            result: Box::new(t.transform_domain(*result)?),
        }),
        CaseArm::Default { result } => Ok(CaseArm::Default {
            result: Box::new(t.transform_domain(*result)?),
        }),
    }
}

// =============================================================================
// Walk functions — core expressions
// =============================================================================

pub fn walk_transform_domain<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    expr: DomainExpression<P>,
) -> Result<DomainExpression<Q>> {
    match expr {
        // Leaf variants — no recursive children, phase-agnostic fields only
        DomainExpression::Lvar {
            name,
            qualifier,
            namespace_path,
            alias,
            provenance, // PhaseBox<Option<LvarProvenance>, Refined> — fixed phase, pass through
        } => Ok(DomainExpression::Lvar {
            name,
            qualifier,
            namespace_path,
            alias,
            provenance,
        }),
        DomainExpression::Literal { value, alias } => {
            Ok(DomainExpression::Literal { value, alias })
        }
        DomainExpression::Projection(proj) => {
            Ok(DomainExpression::Projection(transform_projection(proj)?))
        }
        DomainExpression::NonUnifiyingUnderscore => Ok(DomainExpression::NonUnifiyingUnderscore),
        DomainExpression::ValuePlaceholder { alias } => {
            Ok(DomainExpression::ValuePlaceholder { alias })
        }
        DomainExpression::Substitution(sub) => Ok(DomainExpression::Substitution(sub)),
        DomainExpression::ColumnOrdinal(ordinal) => {
            Ok(DomainExpression::ColumnOrdinal(ordinal.rephase()))
        }

        // Recursive variants
        DomainExpression::Function(f) => Ok(DomainExpression::Function(t.transform_function(f)?)),
        DomainExpression::Predicate { expr, alias } => Ok(DomainExpression::Predicate {
            expr: Box::new(t.transform_boolean(*expr)?),
            alias,
        }),
        DomainExpression::PipedExpression {
            value,
            transforms,
            alias,
        } => Ok(DomainExpression::PipedExpression {
            value: Box::new(t.transform_domain(*value)?),
            transforms: transforms
                .into_iter()
                .map(|f| t.transform_function(f))
                .collect::<Result<Vec<_>>>()?,
            alias,
        }),
        DomainExpression::Parenthesized { inner, alias } => Ok(DomainExpression::Parenthesized {
            inner: Box::new(t.transform_domain(*inner)?),
            alias,
        }),
        DomainExpression::Tuple { elements, alias } => Ok(DomainExpression::Tuple {
            elements: elements
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            alias,
        }),
        DomainExpression::ScalarSubquery {
            identifier,
            subquery,
            alias,
        } => Ok(DomainExpression::ScalarSubquery {
            identifier,
            subquery: Box::new(t.transform_relational_action(*subquery)?.into_inner()),
            alias,
        }),
        DomainExpression::PivotOf {
            value_column,
            pivot_key,
            pivot_values,
        } => Ok(DomainExpression::PivotOf {
            value_column: Box::new(t.transform_domain(*value_column)?),
            pivot_key: Box::new(t.transform_domain(*pivot_key)?),
            pivot_values,
        }),
    }
}

pub fn walk_transform_boolean<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    expr: BooleanExpression<P>,
) -> Result<BooleanExpression<Q>> {
    match expr {
        // Leaf variants
        BooleanExpression::Using { columns } => Ok(BooleanExpression::Using { columns }),
        BooleanExpression::BooleanLiteral { value } => {
            Ok(BooleanExpression::BooleanLiteral { value })
        }
        BooleanExpression::GlobCorrelation { left, right } => {
            Ok(BooleanExpression::GlobCorrelation { left, right })
        }
        BooleanExpression::OrdinalGlobCorrelation { left, right } => {
            Ok(BooleanExpression::OrdinalGlobCorrelation { left, right })
        }

        // Recursive variants
        BooleanExpression::Comparison {
            operator,
            left,
            right,
        } => Ok(BooleanExpression::Comparison {
            operator,
            left: Box::new(t.transform_domain(*left)?),
            right: Box::new(t.transform_domain(*right)?),
        }),
        BooleanExpression::And { left, right } => Ok(BooleanExpression::And {
            left: Box::new(t.transform_boolean(*left)?),
            right: Box::new(t.transform_boolean(*right)?),
        }),
        BooleanExpression::Or { left, right } => Ok(BooleanExpression::Or {
            left: Box::new(t.transform_boolean(*left)?),
            right: Box::new(t.transform_boolean(*right)?),
        }),
        BooleanExpression::Not { expr } => Ok(BooleanExpression::Not {
            expr: Box::new(t.transform_boolean(*expr)?),
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
            subquery: Box::new(t.transform_relational_action(*subquery)?.into_inner()),
            alias,
            using_columns,
        }),
        BooleanExpression::In {
            value,
            set,
            negated,
        } => Ok(BooleanExpression::In {
            value: Box::new(t.transform_domain(*value)?),
            set: set
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            negated,
        }),
        BooleanExpression::InRelational {
            value,
            subquery,
            identifier,
            negated,
        } => Ok(BooleanExpression::InRelational {
            value: Box::new(t.transform_domain(*value)?),
            subquery: Box::new(t.transform_relational_action(*subquery)?.into_inner()),
            identifier,
            negated,
        }),
        BooleanExpression::Sigma { condition } => Ok(BooleanExpression::Sigma {
            condition: Box::new(t.transform_sigma(*condition)?),
        }),
    }
}

pub fn walk_transform_function<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    func: FunctionExpression<P>,
) -> Result<FunctionExpression<Q>> {
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
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            alias,
            conditioned_on: conditioned_on
                .map(|c| t.transform_boolean(*c).map(|b| Box::new(b)))
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
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            conditioned_on: conditioned_on
                .map(|c| t.transform_boolean(*c).map(|b| Box::new(b)))
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
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            regular_arguments: regular_arguments
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            alias,
            conditioned_on: conditioned_on
                .map(|c| t.transform_boolean(*c).map(|b| Box::new(b)))
                .transpose()?,
        }),
        FunctionExpression::Bracket { arguments, alias } => Ok(FunctionExpression::Bracket {
            arguments: arguments
                .into_iter()
                .map(|e| t.transform_domain(e))
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
                .map(|m| t.transform_curly_member(m))
                .collect::<Result<Vec<_>>>()?,
            inner_grouping_keys: inner_grouping_keys
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            cte_requirements: cte_requirements
                .map(|r| transform_cte_requirements(t, r))
                .transpose()?,
            alias,
        }),
        FunctionExpression::Array { members, alias } => Ok(FunctionExpression::Array {
            members: members
                .into_iter()
                .map(|m| t.transform_array_member(m))
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
            constructor: Box::new(t.transform_function(*constructor)?),
            keys_only,
            cte_requirements: cte_requirements
                .map(|r| transform_cte_requirements(t, r))
                .transpose()?,
            alias,
        }),
        FunctionExpression::Lambda { body, alias } => Ok(FunctionExpression::Lambda {
            body: Box::new(t.transform_domain(*body)?),
            alias,
        }),
        FunctionExpression::Infix {
            operator,
            left,
            right,
            alias,
        } => Ok(FunctionExpression::Infix {
            operator,
            left: Box::new(t.transform_domain(*left)?),
            right: Box::new(t.transform_domain(*right)?),
            alias,
        }),
        FunctionExpression::StringTemplate { parts, alias } => {
            Ok(FunctionExpression::StringTemplate {
                parts: parts
                    .into_iter()
                    .map(|p| t.transform_string_template_part(p))
                    .collect::<Result<Vec<_>>>()?,
                alias,
            })
        }
        FunctionExpression::CaseExpression { arms, alias } => {
            Ok(FunctionExpression::CaseExpression {
                arms: arms
                    .into_iter()
                    .map(|a| t.transform_case_arm(a))
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
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            partition_by: partition_by
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            order_by: order_by
                .into_iter()
                .map(|o| t.transform_ordering_spec(o))
                .collect::<Result<Vec<_>>>()?,
            frame: frame.map(|f| t.transform_window_frame(f)).transpose()?,
            alias,
        }),
        FunctionExpression::JsonPath {
            source,
            path,
            alias,
        } => Ok(FunctionExpression::JsonPath {
            source: Box::new(t.transform_domain(*source)?),
            path: Box::new(t.transform_domain(*path)?),
            alias,
        }),
    }
}

// =============================================================================
// Walk functions — sigma, operator, pipe, inner_relation
// =============================================================================

pub fn walk_transform_sigma<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    cond: SigmaCondition<P>,
) -> Result<SigmaCondition<Q>> {
    match cond {
        SigmaCondition::Predicate(pred) => {
            Ok(SigmaCondition::Predicate(t.transform_boolean(pred)?))
        }
        SigmaCondition::TupleOrdinal(clause) => Ok(SigmaCondition::TupleOrdinal(clause)),
        SigmaCondition::Destructure {
            json_column,
            pattern,
            mode,
            destructured_schema,
        } => Ok(SigmaCondition::Destructure {
            json_column: Box::new(t.transform_domain(*json_column)?),
            pattern: Box::new(t.transform_function(*pattern)?),
            mode,
            destructured_schema: destructured_schema.rephase(),
        }),
        SigmaCondition::SigmaCall {
            functor,
            arguments,
            exists,
        } => Ok(SigmaCondition::SigmaCall {
            functor,
            arguments: arguments
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            exists,
        }),
    }
}

pub fn walk_transform_operator<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    op: UnaryRelationalOperator<P>,
) -> Result<UnaryRelationalOperator<Q>> {
    match op {
        UnaryRelationalOperator::General {
            containment_semantic,
            expressions,
        } => Ok(UnaryRelationalOperator::General {
            containment_semantic,
            expressions: expressions
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
        }),
        UnaryRelationalOperator::Modulo {
            containment_semantic,
            spec,
        } => Ok(UnaryRelationalOperator::Modulo {
            containment_semantic,
            spec: t.transform_modulo_spec(spec)?,
        }),
        UnaryRelationalOperator::TupleOrdering {
            containment_semantic,
            specs,
        } => Ok(UnaryRelationalOperator::TupleOrdering {
            containment_semantic,
            specs: specs
                .into_iter()
                .map(|s| t.transform_ordering_spec(s))
                .collect::<Result<Vec<_>>>()?,
        }),
        UnaryRelationalOperator::MapCover {
            function,
            columns,
            containment_semantic,
            conditioned_on,
        } => Ok(UnaryRelationalOperator::MapCover {
            function: t.transform_function(function)?,
            columns: columns
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            containment_semantic,
            conditioned_on: conditioned_on
                .map(|c| t.transform_boolean(*c).map(|b| Box::new(b)))
                .transpose()?,
        }),
        UnaryRelationalOperator::ProjectOut {
            containment_semantic,
            expressions,
        } => Ok(UnaryRelationalOperator::ProjectOut {
            containment_semantic,
            expressions: expressions
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
        }),
        UnaryRelationalOperator::RenameCover { specs } => {
            Ok(UnaryRelationalOperator::RenameCover {
                specs: specs
                    .into_iter()
                    .map(|s| t.transform_rename_spec(s))
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        UnaryRelationalOperator::Transform {
            transformations,
            conditioned_on,
        } => Ok(UnaryRelationalOperator::Transform {
            transformations: transformations
                .into_iter()
                .map(|(expr, alias, qual)| Ok((t.transform_domain(expr)?, alias, qual)))
                .collect::<Result<Vec<_>>>()?,
            conditioned_on: conditioned_on
                .map(|c| t.transform_boolean(*c).map(|b| Box::new(b)))
                .transpose()?,
        }),
        UnaryRelationalOperator::AggregatePipe { aggregations } => {
            Ok(UnaryRelationalOperator::AggregatePipe {
                aggregations: aggregations
                    .into_iter()
                    .map(|e| t.transform_domain(e))
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        UnaryRelationalOperator::Reposition { moves } => Ok(UnaryRelationalOperator::Reposition {
            moves: moves
                .into_iter()
                .map(|m| t.transform_reposition_spec(m))
                .collect::<Result<Vec<_>>>()?,
        }),
        UnaryRelationalOperator::EmbedMapCover {
            function,
            selector,
            alias_template,
            containment_semantic,
        } => Ok(UnaryRelationalOperator::EmbedMapCover {
            function: t.transform_function(function)?,
            selector: t.transform_column_selector(selector)?,
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
            first_parens_spec: first_parens_spec
                .map(|s| t.transform_domain_spec(s))
                .transpose()?,
            domain_spec: t.transform_domain_spec(domain_spec)?,
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
            domain_spec: t.transform_domain_spec(domain_spec)?,
        }),
        // Leaf operators — no recursive children, phase-agnostic
        UnaryRelationalOperator::MetaIze { detailed } => {
            Ok(UnaryRelationalOperator::MetaIze { detailed })
        }
        UnaryRelationalOperator::Witness { exists } => {
            Ok(UnaryRelationalOperator::Witness { exists })
        }
        UnaryRelationalOperator::Qualify => Ok(UnaryRelationalOperator::Qualify),
        UnaryRelationalOperator::Using { columns } => {
            Ok(UnaryRelationalOperator::Using { columns })
        }
        UnaryRelationalOperator::UsingAll => Ok(UnaryRelationalOperator::UsingAll),
        UnaryRelationalOperator::InteriorDrillDown {
            column,
            glob,
            columns,
            interior_schema,
            groundings,
        } => Ok(UnaryRelationalOperator::InteriorDrillDown {
            column,
            glob,
            columns,
            interior_schema,
            groundings,
        }),
        UnaryRelationalOperator::NarrowingDestructure { column, fields } => {
            Ok(UnaryRelationalOperator::NarrowingDestructure { column, fields })
        }
        UnaryRelationalOperator::DirectiveTerminal { name, arguments } => {
            Ok(UnaryRelationalOperator::DirectiveTerminal {
                name,
                arguments: arguments
                    .into_iter()
                    .map(|e| t.transform_domain(e))
                    .collect::<Result<Vec<_>>>()?,
            })
        }
    }
}

pub fn walk_transform_pipe<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    pipe: PipeExpression<P>,
) -> Result<PipeExpression<Q>> {
    Ok(PipeExpression {
        source: t.transform_relational_action(pipe.source)?.into_inner(),
        operator: t.transform_operator(pipe.operator)?,
        cpr_schema: pipe.cpr_schema.rephase(),
    })
}

pub fn walk_transform_inner_relation<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    pattern: InnerRelationPattern<P>,
) -> Result<InnerRelationPattern<Q>> {
    match pattern {
        InnerRelationPattern::Indeterminate {
            identifier,
            subquery,
        } => Ok(InnerRelationPattern::Indeterminate {
            identifier,
            subquery: Box::new(t.transform_relational_action(*subquery)?.into_inner()),
        }),
        InnerRelationPattern::UncorrelatedDerivedTable {
            identifier,
            subquery,
            is_consulted_view,
        } => Ok(InnerRelationPattern::UncorrelatedDerivedTable {
            identifier,
            subquery: Box::new(t.transform_relational_action(*subquery)?.into_inner()),
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
                .map(|f| t.transform_boolean(f))
                .collect::<Result<Vec<_>>>()?,
            subquery: Box::new(t.transform_relational_action(*subquery)?.into_inner()),
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
                .map(|f| t.transform_boolean(f))
                .collect::<Result<Vec<_>>>()?,
            aggregations: aggregations
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            subquery: Box::new(t.transform_relational_action(*subquery)?.into_inner()),
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
                .map(|f| t.transform_boolean(f))
                .collect::<Result<Vec<_>>>()?,
            order_by: order_by
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            limit,
            subquery: Box::new(t.transform_relational_action(*subquery)?.into_inner()),
        }),
    }
}

// =============================================================================
// Walk functions — relational layer
// =============================================================================

pub fn walk_transform_relation<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    rel: Relation<P>,
) -> Result<Relation<Q>> {
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
            canonical_name: canonical_name.rephase(),
            domain_spec: t.transform_domain_spec(domain_spec)?,
            alias,
            outer,
            mutation_target,
            passthrough,
            cpr_schema: cpr_schema.rephase(),
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
                        .map(|h| t.transform_domain(h))
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?,
            rows: rows
                .into_iter()
                .map(|r| t.transform_row(r))
                .collect::<Result<Vec<_>>>()?,
            alias,
            outer,
            exists_mode,
            qua_target,
            cpr_schema: cpr_schema.rephase(),
        }),
        Relation::TVF {
            function,
            argument_groups,
            domain_spec,
            alias,
            namespace,
            grounding,
            cpr_schema,
            ho_arguments,
        } => Ok(Relation::TVF {
            function,
            argument_groups,
            domain_spec: t.transform_domain_spec(domain_spec)?,
            alias,
            namespace,
            grounding,
            cpr_schema: cpr_schema.rephase(),
            ho_arguments: ho_arguments
                .into_iter()
                .map(|a| match a {
                    crate::pipeline::asts::core::operators::HoArgument::Table(r) => {
                        t.transform_relational(r).map(crate::pipeline::asts::core::operators::HoArgument::Table)
                    }
                    crate::pipeline::asts::core::operators::HoArgument::Scalar(d) => {
                        t.transform_domain(d).map(crate::pipeline::asts::core::operators::HoArgument::Scalar)
                    }
                })
                .collect::<Result<Vec<_>>>()?,
        }),
        Relation::InnerRelation {
            pattern,
            alias,
            outer,
            cpr_schema,
        } => Ok(Relation::InnerRelation {
            pattern: t.transform_inner_relation(pattern)?,
            alias,
            outer,
            cpr_schema: cpr_schema.rephase(),
        }),
        Relation::ConsultedView {
            identifier,
            body,
            scoped,
            outer,
        } => Ok(Relation::ConsultedView {
            identifier,
            body: Box::new(t.transform_query(*body)?),
            scoped: scoped.rephase(),
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
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            alias,
            cpr_schema: cpr_schema.rephase(),
        }),
    }
}

#[stacksafe::stacksafe]
pub fn walk_transform_relational<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    expr: RelationalExpression<P>,
) -> Result<RelationalExpression<Q>> {
    match expr {
        RelationalExpression::Relation(rel) => Ok(RelationalExpression::Relation(
            t.transform_relation_action(rel)?.into_inner(),
        )),
        RelationalExpression::Join {
            left,
            right,
            join_condition,
            join_type,
            cpr_schema,
        } => Ok(RelationalExpression::Join {
            left: Box::new(t.transform_relational_action(*left)?.into_inner()),
            right: Box::new(t.transform_relational_action(*right)?.into_inner()),
            join_condition: join_condition.map(|c| t.transform_boolean(c)).transpose()?,
            join_type,
            cpr_schema: cpr_schema.rephase(),
        }),
        RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema,
        } => Ok(RelationalExpression::Filter {
            source: Box::new(t.transform_relational_action(*source)?.into_inner()),
            condition: t.transform_sigma(condition)?,
            origin,
            cpr_schema: cpr_schema.rephase(),
        }),
        RelationalExpression::Pipe(pipe) => Ok(RelationalExpression::Pipe(Box::new(
            stacksafe::StackSafe::new(t.transform_pipe((*pipe).into_inner())?),
        ))),
        RelationalExpression::SetOperation {
            operator,
            operands,
            correlation: _, // PhaseBox<Option<BooleanExpression<P>>, P> — inner type is phase-parameterized, must use phantom
            cpr_schema,
        } => Ok(RelationalExpression::SetOperation {
            operator,
            operands: operands
                .into_iter()
                .map(|e| Ok(t.transform_relational_action(e)?.into_inner()))
                .collect::<Result<Vec<_>>>()?,
            correlation: PhaseBox::phantom(),
            cpr_schema: cpr_schema.rephase(),
        }),
        RelationalExpression::ErJoinChain { relations } => Ok(RelationalExpression::ErJoinChain {
            relations: relations
                .into_iter()
                .map(|r| Ok(t.transform_relation_action(r)?.into_inner()))
                .collect::<Result<Vec<_>>>()?,
        }),
        RelationalExpression::ErTransitiveJoin { left, right } => {
            Ok(RelationalExpression::ErTransitiveJoin {
                left: Box::new(t.transform_relational_action(*left)?.into_inner()),
                right: Box::new(t.transform_relational_action(*right)?.into_inner()),
            })
        }
    }
}

// =============================================================================
// Walk functions — top-level
// =============================================================================

pub fn walk_transform_cte_binding<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    cte: CteBinding<P>,
) -> Result<CteBinding<Q>> {
    Ok(CteBinding {
        expression: t.transform_relational_action(cte.expression)?.into_inner(),
        name: cte.name,
        is_recursive: cte.is_recursive.rephase(),
    })
}

pub fn walk_transform_query<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    query: Query<P>,
) -> Result<Query<Q>> {
    match query {
        Query::Relational(expr) => Ok(Query::Relational(
            t.transform_relational_action(expr)?.into_inner(),
        )),
        Query::WithCtes { ctes, query } => Ok(Query::WithCtes {
            ctes: ctes
                .into_iter()
                .map(|c| t.transform_cte_binding(c))
                .collect::<Result<Vec<_>>>()?,
            query: t.transform_relational_action(query)?.into_inner(),
        }),
        Query::WithCfes { cfes, query } => Ok(Query::WithCfes {
            cfes,
            query: Box::new(t.transform_query(*query)?),
        }),
        Query::WithPrecompiledCfes { cfes, query } => Ok(Query::WithPrecompiledCfes {
            cfes,
            query: Box::new(t.transform_query(*query)?),
        }),
        Query::ReplTempTable { query, table_name } => Ok(Query::ReplTempTable {
            query: Box::new(t.transform_query(*query)?),
            table_name,
        }),
        Query::WithErContext { context, query } => Ok(Query::WithErContext {
            context,
            query: Box::new(t.transform_query(*query)?),
        }),
        Query::ReplTempView { query, view_name } => Ok(Query::ReplTempView {
            query: Box::new(t.transform_query(*query)?),
            view_name,
        }),
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Transform CteRequirements from phase P to phase Q.
/// Transforms internal DomainExpression fields and uses phantom for PhaseBox.
fn transform_cte_requirements<P, Q, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    reqs: CteRequirements<P>,
) -> Result<CteRequirements<Q>> {
    Ok(CteRequirements {
        needs_cte: reqs.needs_cte,
        accumulated_grouping_keys: reqs
            .accumulated_grouping_keys
            .into_iter()
            .map(|(name, expr)| Ok((name, t.transform_domain(expr)?)))
            .collect::<Result<Vec<_>>>()?,
        join_keys: reqs
            .join_keys
            .into_iter()
            .map(|e| t.transform_domain(e))
            .collect::<Result<Vec<_>>>()?,
        location: reqs.location,
        nested_members_info: reqs.nested_members_info,
        cte_name: reqs.cte_name.rephase(),
    })
}

/// Transform ProjectionExpr from phase P to phase Q.
/// Leaf variants pass through; ColumnRange uses phantom.
fn transform_projection<P, Q>(
    proj: crate::pipeline::asts::core::expressions::domain::ProjectionExpr<P>,
) -> Result<crate::pipeline::asts::core::expressions::domain::ProjectionExpr<Q>> {
    use crate::pipeline::asts::core::expressions::domain::ProjectionExpr;
    match proj {
        ProjectionExpr::Glob {
            qualifier,
            namespace_path,
        } => Ok(ProjectionExpr::Glob {
            qualifier,
            namespace_path,
        }),
        ProjectionExpr::ColumnRange(range) => Ok(ProjectionExpr::ColumnRange(range.rephase())),
        ProjectionExpr::Pattern { pattern, alias } => {
            Ok(ProjectionExpr::Pattern { pattern, alias })
        }
        ProjectionExpr::JsonPathLiteral {
            segments,
            root_is_array,
            alias,
        } => Ok(ProjectionExpr::JsonPathLiteral {
            segments,
            root_is_array,
            alias,
        }),
    }
}
