// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Smart constructors for AST expression types
// Provides fluent builder APIs to reduce boilerplate in AST construction

use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::core::{
    AuthoredColumn, Chain, Continuation, DomainExpression,
    FunctionApplication, LiteralValue, Phase, RenameSpec, TruthExpression,
    PipeOp, Unresolved,
};
use crate::pipeline::asts::core::{NamedReference, Reference, SelectorItem};
use delightql_types::SqlIdentifier;

// ============================================================================
// DomainExpression Builders
// ============================================================================

impl DomainExpression<Unresolved> {
    /// A written column reference. Only the authored phase has one to write:
    /// after resolution a column is an identity, and there is no door here
    /// that turns characters back into one.
    pub fn lvar_builder(name: impl Into<SqlIdentifier>) -> LvarBuilder {
        LvarBuilder {
            name: name.into(),
            qualifier: None,
            namespace_path: vec![],
        }
    }
}

impl<P: Phase> DomainExpression<P> {
    pub fn literal_builder(value: LiteralValue) -> LiteralBuilder {
        LiteralBuilder { value }
    }

    pub fn predicate_builder(expr: TruthExpression) -> PredicateBuilder {
        PredicateBuilder {
            expr: Box::new(expr),
        }
    }

    pub fn placeholder_builder() -> PlaceholderBuilder {
        PlaceholderBuilder
    }
}

// ============================================================================
// Builder Structs
// ============================================================================

pub struct LvarBuilder {
    name: SqlIdentifier,
    qualifier: Option<SqlIdentifier>,
    namespace_path: Vec<String>,
}

impl LvarBuilder {
    /// Set the qualifier, preserving the caller's SqlIdentifier (stroppedness
    /// survives). `String`/`&str` land unstropped via the From impls.
    pub fn with_qualifier(mut self, qualifier: impl Into<SqlIdentifier>) -> Self {
        self.qualifier = Some(qualifier.into());
        self
    }

    /// Optional-qualifier form: for callers that already hold an
    /// `Option<SqlIdentifier>` (e.g. parse_lvar). `None` clears the qualifier.
    pub fn with_qualifier_opt(mut self, qualifier: Option<SqlIdentifier>) -> Self {
        self.qualifier = qualifier;
        self
    }

    pub fn with_namespace_path(mut self, namespace_path: Vec<String>) -> Self {
        self.namespace_path = namespace_path;
        self
    }

    pub fn build(self) -> DomainExpression<Unresolved> {
        use crate::pipeline::asts::unresolved::NamespacePath;
        DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn {
            name: self.name,
            qualifier: self.qualifier,
            namespace_path: NamespacePath::from_parts(self.namespace_path)
                .expect("Invalid namespace path"),
        })))
    }
}

pub struct LiteralBuilder {
    value: LiteralValue,
}

impl LiteralBuilder {
    pub fn build(self) -> DomainExpression {
        DomainExpression::Application(super::expressions::FunctionApplication::Ground(self.value))
    }
}

pub struct PredicateBuilder {
    expr: Box<TruthExpression>,
}

impl PredicateBuilder {
    pub fn build(self) -> TruthExpression {
        // Return the boolean expression directly - no wrapping!
        *self.expr
    }
}

// ============================================================================
// TruthExpression Builders
// ============================================================================

impl TruthExpression {
    pub fn comparison(
        op: crate::pipeline::asts::vocabulary::CmpOp,
        left: DomainExpression,
        right: DomainExpression,
    ) -> Self {
        TruthExpression::Comparison(Comparison {
            operator: op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }
}

// THE CANONICAL CONSTRUCTORS ARE `all` AND `any`, and they are the only ones.
// A binary `and`/`or` pair stood here building a two-member `Vec2` directly,
// which rebuilt the same-operator nesting the n-ary carrier exists to make
// impossible: handed a conjunction, it produced a conjunction of one. They
// had no caller; splicing is not an option a second door may decline.

// ============================================================================
// FunctionApplication Builders
// ============================================================================

impl FunctionApplication {
    pub fn function_builder(reference: crate::pipeline::asts::vocabulary::Ref) -> FunctionBuilder {
        FunctionBuilder {
            reference,
            arguments: Vec::new(),
            alias: None,
            is_curried: false,
            conditioned_on: None,
        }
    }
}

pub struct FunctionBuilder {
    reference: crate::pipeline::asts::vocabulary::Ref,
    arguments: Vec<DomainExpression>,
    alias: Option<String>,
    is_curried: bool,
    conditioned_on: Option<Box<TruthExpression>>,
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

    pub fn as_curried(mut self) -> Self {
        self.is_curried = true;
        self
    }

    pub fn with_condition(mut self, condition: TruthExpression) -> Self {
        self.conditioned_on = Some(Box::new(condition));
        self
    }

    pub fn build(self) -> FunctionApplication {
        let call = crate::pipeline::asts::core::FunctorCall::scalar(self.reference, self.arguments);
        let _ = self.is_curried;
        super::expressions::FunctionApplication::Standard(
            crate::pipeline::asts::core::StandardApplication {
                call: crate::pipeline::asts::core::PureCall::seal(call)
                    .expect("scalar function builder only accepts pure references"),
                guard: self.conditioned_on,
                window: None,
            },
        )
    }
}

pub struct PlaceholderBuilder;

impl PlaceholderBuilder {
    pub fn build(self) -> DomainExpression {
        DomainExpression::Application(super::expressions::FunctionApplication::Open(
            super::expressions::DomainHole::Disregarded,
        ))
    }
}

// ============================================================================
// Mini-Kingdom: Binary Predicate Composition
// ============================================================================
// REMOVED - The old and/or methods were incorrectly wrapping predicates
// Now we use the proper TruthExpression::And and TruthExpression::Or variants
// defined above in the main TruthExpression impl block

// ============================================================================
// Kingdom 2: Chain builders
// ============================================================================

impl<P: Phase> Chain<P> {
    /// Apply a pipe operator to this chain.
    pub fn pipe_builder(source: Chain<P>, cpr_schema: P::Scope) -> PipeBuilder<P> {
        PipeBuilder {
            source,
            operator: None,
            cpr_schema,
        }
    }
}

// ============================================================================
// Pipe Builder
// ============================================================================

pub struct PipeBuilder<P: Phase> {
    source: Chain<P>,
    operator: Option<PipeOp<P>>,
    cpr_schema: P::Scope,
}

impl<P: Phase> PipeBuilder<P> {
    /// Add projection operator |> [items]
    pub fn with_projection(
        mut self,
        items: crate::pipeline::asts::vocabulary::Vec1<crate::pipeline::asts::core::OutItem<P>>,
    ) -> Self {
        assert!(self.operator.is_none(), "Pipe operator already set");
        self.operator = Some(PipeOp::Project(items));
        self
    }


    /// Add project out operator |> ^[selector]
    pub fn with_project_out(mut self, selector: Vec<SelectorItem<P>>) -> Self {
        assert!(self.operator.is_none(), "Pipe operator already set");
        self.operator = Some(PipeOp::ProjectOut(selector));
        self
    }

    /// Add rename cover operator |> *(specs)
    pub fn with_rename_cover(
        mut self,
        specs: crate::pipeline::asts::vocabulary::Vec1<RenameSpec<P>>,
    ) -> Self {
        assert!(self.operator.is_none(), "Pipe operator already set");
        self.operator = Some(PipeOp::Rename(specs));
        self
    }

    pub fn build(self) -> Chain<P> {
        self.source.then(Continuation::Pipe {
            operator: self.operator.expect("Pipe must have an operator"),
            named: P::no_stage_name(),
            cpr_schema: self.cpr_schema,
        })
    }
}
