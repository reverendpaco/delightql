// Smart constructors for AST expression types
// Provides fluent builder APIs to reduce boilerplate in AST construction

use crate::pipeline::asts::core::{
    BooleanExpression, ContainmentSemantic, DomainExpression, FilterOrigin, FunctionExpression,
    JoinType, LiteralValue, ModuloSpec, OrderingSpec, PhaseBox, RelationalExpression, RenameSpec,
    SigmaCondition, UnaryRelationalOperator, UsingColumn,
};

// ============================================================================
// DomainExpression Builders
// ============================================================================

impl<Phase> DomainExpression<Phase> {
    pub fn lvar_builder(name: impl Into<String>) -> LvarBuilder<Phase> {
        LvarBuilder {
            name: name.into(),
            qualifier: None,
            namespace_path: vec![],
            alias: None,
            _phase: std::marker::PhantomData,
        }
    }

    pub fn literal_builder(value: LiteralValue) -> LiteralBuilder {
        LiteralBuilder { value, alias: None }
    }

    pub fn predicate_builder(expr: BooleanExpression) -> PredicateBuilder {
        PredicateBuilder {
            expr: Box::new(expr),
            alias: None,
        }
    }

    pub fn glob_builder() -> GlobBuilder {
        GlobBuilder {
            qualifier: None,
            namespace_path: vec![],
        }
    }

    pub fn placeholder_builder() -> PlaceholderBuilder {
        PlaceholderBuilder
    }
}

// ============================================================================
// Builder Structs
// ============================================================================

pub struct LvarBuilder<Phase> {
    name: String,
    qualifier: Option<String>,
    namespace_path: Vec<String>,
    alias: Option<String>,
    _phase: std::marker::PhantomData<Phase>,
}

impl<Phase> LvarBuilder<Phase> {
    pub fn with_qualifier<T: Into<Option<String>>>(mut self, qualifier: T) -> Self {
        self.qualifier = qualifier.into();
        self
    }

    pub fn with_namespace_path(mut self, namespace_path: Vec<String>) -> Self {
        self.namespace_path = namespace_path;
        self
    }

    pub fn with_alias<T: Into<Option<String>>>(mut self, alias: T) -> Self {
        self.alias = alias.into();
        self
    }

    pub fn build(self) -> DomainExpression<Phase> {
        use crate::pipeline::asts::unresolved::NamespacePath;
        DomainExpression::Lvar {
            name: self.name.into(),
            qualifier: self.qualifier.map(|s| s.into()),
            namespace_path: NamespacePath::from_parts(self.namespace_path)
                .expect("Invalid namespace path"),
            alias: self.alias.map(|s| s.into()),
            provenance: PhaseBox::phantom(),
        }
    }
}

pub struct LiteralBuilder {
    value: LiteralValue,
    alias: Option<String>,
}

impl LiteralBuilder {
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    pub fn build(self) -> DomainExpression {
        DomainExpression::Literal {
            value: self.value,
            alias: self.alias.map(|s| s.into()),
        }
    }
}

pub struct PredicateBuilder {
    expr: Box<BooleanExpression>,
    alias: Option<String>,
}

impl PredicateBuilder {
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    pub fn build(self) -> BooleanExpression {
        // Return the boolean expression directly - no wrapping!
        *self.expr
    }
}

// ============================================================================
// BooleanExpression Builders
// ============================================================================

impl BooleanExpression {
    pub fn comparison(
        op: impl Into<String>,
        left: DomainExpression,
        right: DomainExpression,
    ) -> Self {
        BooleanExpression::Comparison {
            operator: op.into(),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn and(left: BooleanExpression, right: BooleanExpression) -> Self {
        BooleanExpression::And {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn or(left: BooleanExpression, right: BooleanExpression) -> Self {
        BooleanExpression::Or {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn using(columns: Vec<UsingColumn>) -> Self {
        BooleanExpression::Using { columns }
    }
}

// ============================================================================
// FunctionExpression Builders
// ============================================================================

impl FunctionExpression {
    pub fn function_builder(name: impl Into<String>) -> FunctionBuilder {
        FunctionBuilder {
            name: name.into(),
            namespace: None,
            arguments: Vec::new(),
            alias: None,
            is_curried: false,
            conditioned_on: None,
        }
    }

    pub fn infix(op: impl Into<String>, left: DomainExpression, right: DomainExpression) -> Self {
        FunctionExpression::Infix {
            operator: op.into(),
            left: Box::new(left),
            right: Box::new(right),
            alias: None,
        }
    }
}

pub struct FunctionBuilder {
    name: String,
    namespace: Option<crate::pipeline::asts::core::metadata::NamespacePath>,
    arguments: Vec<DomainExpression>,
    alias: Option<String>,
    is_curried: bool,
    conditioned_on: Option<Box<BooleanExpression>>,
}

impl FunctionBuilder {
    pub fn add_arg(mut self, arg: DomainExpression) -> Self {
        self.arguments.push(arg);
        self
    }

    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    pub fn with_namespace(
        mut self,
        namespace: Option<crate::pipeline::asts::core::metadata::NamespacePath>,
    ) -> Self {
        self.namespace = namespace;
        self
    }

    pub fn as_curried(mut self) -> Self {
        self.is_curried = true;
        self
    }

    pub fn with_condition(mut self, condition: BooleanExpression) -> Self {
        self.conditioned_on = Some(Box::new(condition));
        self
    }

    pub fn build(self) -> FunctionExpression {
        if self.is_curried {
            FunctionExpression::Curried {
                name: self.name.into(),
                namespace: self.namespace,
                arguments: self.arguments,
                conditioned_on: self.conditioned_on,
            }
        } else {
            FunctionExpression::Regular {
                name: self.name.into(),
                namespace: self.namespace,
                arguments: self.arguments,
                alias: self.alias.map(|s| s.into()),
                conditioned_on: self.conditioned_on,
            }
        }
    }
}

pub struct GlobBuilder {
    qualifier: Option<String>,
    namespace_path: Vec<String>,
}

impl GlobBuilder {
    pub fn with_qualifier(mut self, qualifier: impl Into<String>) -> Self {
        self.qualifier = Some(qualifier.into());
        self
    }

    pub fn with_namespace_path(mut self, namespace_path: Vec<String>) -> Self {
        self.namespace_path = namespace_path;
        self
    }

    pub fn build(self) -> DomainExpression {
        use crate::pipeline::asts::core::expressions::domain::ProjectionExpr;
        use crate::pipeline::asts::unresolved::NamespacePath;
        DomainExpression::Projection(ProjectionExpr::Glob {
            qualifier: self.qualifier.map(|s| s.into()),
            namespace_path: NamespacePath::from_parts(self.namespace_path)
                .expect("Invalid namespace path"),
        })
    }
}

pub struct PlaceholderBuilder;

impl PlaceholderBuilder {
    pub fn build(self) -> DomainExpression {
        DomainExpression::NonUnifiyingUnderscore
    }
}

// ============================================================================
// Mini-Kingdom: Binary Predicate Composition
// ============================================================================
// REMOVED - The old and/or methods were incorrectly wrapping predicates
// Now we use the proper BooleanExpression::And and BooleanExpression::Or variants
// defined above in the main BooleanExpression impl block

// ============================================================================
// Kingdom 2: RelationalExpression Builders
// ============================================================================

impl<Phase> RelationalExpression<Phase> {
    /// Create a join builder
    pub fn join_builder(
        left: RelationalExpression<Phase>,
        right: RelationalExpression<Phase>,
    ) -> JoinBuilder<Phase> {
        JoinBuilder {
            left: Box::new(left),
            right: Box::new(right),
            join_condition: None,
            join_type: None,
            cpr_schema: crate::pipeline::asts::core::PhaseBox::phantom(),
            _phase: std::marker::PhantomData,
        }
    }

    /// Create a filter builder  
    pub fn filter_builder(source: RelationalExpression<Phase>) -> FilterBuilder<Phase> {
        FilterBuilder {
            source: Box::new(source),
            condition: None,
            cpr_schema: crate::pipeline::asts::core::PhaseBox::phantom(),
            _phase: std::marker::PhantomData,
        }
    }

    /// Create a pipe builder
    pub fn pipe_builder(source: RelationalExpression<Phase>) -> PipeBuilder<Phase> {
        PipeBuilder {
            source,
            operator: None,
            cpr_schema: crate::pipeline::asts::core::PhaseBox::phantom(),
            _phase: std::marker::PhantomData,
        }
    }
}

// ============================================================================
// Join Builder
// ============================================================================

pub struct JoinBuilder<Phase> {
    left: Box<RelationalExpression<Phase>>,
    right: Box<RelationalExpression<Phase>>,
    join_condition: Option<BooleanExpression<Phase>>,
    join_type: Option<JoinType>,
    cpr_schema:
        crate::pipeline::asts::core::PhaseBox<crate::pipeline::asts::core::CprSchema, Phase>,
    _phase: std::marker::PhantomData<Phase>,
}

impl<Phase> JoinBuilder<Phase> {
    /// Add ON condition (mutually exclusive with using/natural)
    pub fn with_on(mut self, condition: SigmaCondition<Phase>) -> Self {
        assert!(self.join_condition.is_none(), "Join condition already set");
        self.join_condition = Some(match condition {
            SigmaCondition::Predicate(expr) => expr,
            _ => panic!("Only predicate conditions supported in ON clause"), // TODO: Better error handling
        });
        self
    }

    /// Add USING condition (mutually exclusive with on/natural)
    pub fn with_using(mut self, condition: SigmaCondition<Phase>) -> Self {
        assert!(self.join_condition.is_none(), "Join condition already set");
        self.join_condition = Some(match condition {
            SigmaCondition::Predicate(expr) => expr,
            _ => panic!("Only predicate conditions supported in USING clause"), // TODO: Better error handling
        });
        self
    }

    /// Add a USING expression directly (mutually exclusive with on/natural/with_using)
    pub fn with_using_expr(mut self, expr: BooleanExpression<Phase>) -> Self {
        assert!(self.join_condition.is_none(), "Join condition already set");
        self.join_condition = Some(expr);
        self
    }

    /// Natural join (mutually exclusive with on/using)
    pub fn natural(self) -> Self {
        assert!(self.join_condition.is_none(), "Join condition already set");
        // Natural joins have no explicit condition
        self
    }

    /// Set join type
    pub fn with_join_type(mut self, join_type: JoinType) -> Self {
        self.join_type = Some(join_type);
        self
    }

    pub fn build(self) -> RelationalExpression<Phase> {
        RelationalExpression::Join {
            left: self.left,
            right: self.right,
            join_condition: self.join_condition,
            join_type: self.join_type,
            cpr_schema: self.cpr_schema,
        }
    }
}

// ============================================================================
// Filter Builder
// ============================================================================

pub struct FilterBuilder<Phase> {
    source: Box<RelationalExpression<Phase>>,
    condition: Option<SigmaCondition<Phase>>,
    cpr_schema:
        crate::pipeline::asts::core::PhaseBox<crate::pipeline::asts::core::CprSchema, Phase>,
    _phase: std::marker::PhantomData<Phase>,
}

impl<Phase> FilterBuilder<Phase> {
    /// Add WHERE condition
    pub fn with_condition(mut self, condition: SigmaCondition<Phase>) -> Self {
        assert!(self.condition.is_none(), "Filter condition already set");
        self.condition = Some(condition);
        self
    }

    pub fn build(self) -> RelationalExpression<Phase> {
        RelationalExpression::Filter {
            source: self.source,
            condition: self.condition.expect("Filter must have a condition"),
            origin: FilterOrigin::default(),
            cpr_schema: self.cpr_schema,
        }
    }
}

// ============================================================================
// Pipe Builder
// ============================================================================

pub struct PipeBuilder<Phase> {
    source: RelationalExpression<Phase>,
    operator: Option<UnaryRelationalOperator<Phase>>,
    cpr_schema:
        crate::pipeline::asts::core::PhaseBox<crate::pipeline::asts::core::CprSchema, Phase>,
    _phase: std::marker::PhantomData<Phase>,
}

impl<Phase> PipeBuilder<Phase> {
    /// Add projection operator |> [expressions]
    pub fn with_projection(mut self, expressions: Vec<DomainExpression<Phase>>) -> Self {
        assert!(self.operator.is_none(), "Pipe operator already set");
        self.operator = Some(UnaryRelationalOperator::General {
            containment_semantic: ContainmentSemantic::Bracket,
            expressions,
        });
        self
    }

    /// Add grouping operator |> %(reducing_by)
    pub fn with_grouping(
        mut self,
        reducing_by: Vec<DomainExpression<Phase>>,
        reducing_on: Vec<DomainExpression<Phase>>,
    ) -> Self {
        assert!(self.operator.is_none(), "Pipe operator already set");
        self.operator = Some(UnaryRelationalOperator::Modulo {
            containment_semantic: ContainmentSemantic::Parenthesis,
            spec: ModuloSpec::GroupBy {
                reducing_by,
                reducing_on,
                arbitrary: vec![], // Default to empty
            },
        });
        self
    }

    /// Add ordering operator |> #(specs)
    pub fn with_ordering(mut self, specs: Vec<OrderingSpec<Phase>>) -> Self {
        assert!(self.operator.is_none(), "Pipe operator already set");
        self.operator = Some(UnaryRelationalOperator::TupleOrdering {
            containment_semantic: ContainmentSemantic::Parenthesis,
            specs,
        });
        self
    }

    /// Add project out operator |> ^[expressions]
    pub fn with_project_out(mut self, expressions: Vec<DomainExpression<Phase>>) -> Self {
        assert!(self.operator.is_none(), "Pipe operator already set");
        self.operator = Some(UnaryRelationalOperator::ProjectOut {
            containment_semantic: ContainmentSemantic::Bracket,
            expressions,
        });
        self
    }

    /// Add rename cover operator |> *(specs)
    pub fn with_rename_cover(mut self, specs: Vec<RenameSpec<Phase>>) -> Self {
        assert!(self.operator.is_none(), "Pipe operator already set");
        self.operator = Some(UnaryRelationalOperator::RenameCover { specs });
        self
    }

    pub fn build(self) -> RelationalExpression<Phase> {
        use crate::pipeline::asts::core::PipeExpression;
        RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
            source: self.source,
            operator: self.operator.expect("Pipe must have an operator"),
            cpr_schema: self.cpr_schema,
        })))
    }
}
