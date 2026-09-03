// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Specification types for various operations

use super::expressions::{RenameSource, Spread};
use super::{DomainExpression, Phase, Unresolved};
use crate::{lispy::ToLispy, ToLispy};
use delightql_types::SqlIdentifier;

/// WHICH ROWS ARE EQUIVALENT, and what each equivalence class publishes.
/// A group without reductions is a distinct — `~>` is what separates the
/// two spellings, and normalization constructs the exact one it read.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum GroupSpec<P: Phase = Unresolved> {
    /// `%(keys)` — each class publishes its keys, once. The grammar
    /// refuses `%()`, and the carrier says it too.
    #[lispy("group_spec:distinct")]
    Distinct {
        keys: crate::pipeline::asts::vocabulary::Vec1<OutItem<P>>,
    },
    /// `%(keys ~> reductions)` — each class publishes its keys and one
    /// value per reduction. Zero keys is the singleton reduction: one
    /// class holding every row. REDUCTION POSITION IS NONEMPTY: a
    /// delegate is a reduction item like any other, so a delegate-only
    /// reduce is one or more `ReductionItem::Delegate` members, and an
    /// empty reduction is unspellable rather than checked.
    #[lispy("group_spec:reduce")]
    Reduce {
        keys: Vec<OutItem<P>>,
        reductions: crate::pipeline::asts::vocabulary::Vec1<ReductionItem<P>>,
        /// Analysis owned by this reduction, empty before resolution.
        plan: super::expressions::ReductionPlan<P>,
    },
}

/// WHAT STANDS IN REDUCTION POSITION.
///
/// A reduction publishes one column per item, and the two things that can
/// publish one are not the same kind: an out item computes a VALUE, while a
/// metadata group turns a column's values into an interior record's KEYS.
/// The group is not an expression and cannot be read as one, which is why
/// the position states both admissions in its own type. The delegate is a
/// member too: it publishes its payload from a representative row, and
/// carrying it here is what makes a delegate-only reduce nonempty by
/// construction.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum ReductionItem<P: Phase = Unresolved> {
    #[lispy("reduction_item:out")]
    Out(OutItem<P>),
    #[lispy("reduction_item:metadata")]
    Metadata(MetadataOut<P>),
    /// `(payload) <~ [#(order)]` — a delegate selection: pull the
    /// reduction back to a representative row. A reduction item like any
    /// other, so a delegate-only reduce is nonempty by construction.
    #[lispy("reduction_item:delegate")]
    Delegate(DelegateSpec<P>),
    /// `score of subject` — REDUCTION POSITION ONLY. A pivot rotates one
    /// column's values into columns of their own, so it is not one value and
    /// never was: nothing outside a group can spell it, and no scalar walk
    /// owes it an arm.
    #[lispy("reduction_item:pivot")]
    Pivot(PivotSpec<P>),
}

/// THE IN IS THE HEADING WITNESS.
///
/// A pivot publishes one column per value the key's authored membership
/// predicate named, in the order the author wrote them, so `values` is the
/// heading itself — resolution reads it off that predicate, and nothing
/// about the data decides it. The item publishes no single output of its
/// own, which is why there is no `output` beside `values`.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("pivot_spec")]
pub struct PivotSpec<P: Phase = Unresolved> {
    pub value_column: Box<DomainExpression<P>>,
    pub pivot_key: Box<DomainExpression<P>>,
    /// Empty as authored; resolution fills it from the IN predicate.
    pub values: Vec<String>,
}

/// A metadata group standing in reduction position, with what it publishes.
///
/// Naming and the output occurrence belong to the POSITION, exactly as they
/// do for an out item; the group itself carries only its key and its target.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("metadata_out")]
pub struct MetadataOut<P: Phase = Unresolved> {
    pub group: super::expressions::MetadataGroup<P>,
    /// The `as` the author wrote. `None` is unnamed, not anonymous.
    pub naming: Option<SqlIdentifier>,
    /// PRIVATE, for the reason [`OneOut::output`] states.
    output: P::Output,
}

impl<P: Phase> MetadataOut<P> {
    /// The column this position publishes.
    pub fn output(&self) -> &P::Output {
        &self.output
    }
}

impl<P: Phase> MetadataOut<P> {
    /// Cross a phase boundary. See [`OneOut::folded`].
    pub fn folded<Q: Phase, F: crate::pipeline::ast_transform::AstTransform<P, Q> + ?Sized>(
        self,
        walk: &mut F,
        group: super::expressions::MetadataGroup<Q>,
    ) -> crate::error::Result<MetadataOut<Q>> {
        Ok(MetadataOut {
            group,
            naming: self.naming,
            output: walk.fold_output(self.output)?,
        })
    }
}

impl<P: Phase<Output = ()>> MetadataOut<P> {
    /// The authored position. Nothing is paired — the phase has no port.
    pub fn authored(
        group: super::expressions::MetadataGroup<P>,
        naming: Option<SqlIdentifier>,
    ) -> Self {
        MetadataOut {
            group,
            naming,
            output: (),
        }
    }
}

impl<P: Phase<Output = crate::relation::PortId>> MetadataOut<P> {
    /// THE ONE BOUND-PHASE CONSTRUCTOR, and it is the authority's. The
    /// port is the one the derivation made for THIS position.
    pub(crate) fn published(
        _authority: &crate::relation::builder::SemanticConstruction,
        group: super::expressions::MetadataGroup<P>,
        naming: Option<SqlIdentifier>,
        output: crate::relation::PortId,
    ) -> Self {
        MetadataOut {
            group,
            naming,
            output,
        }
    }

}


impl<P: Phase> ReductionItem<P> {
    /// The column this item publishes, once the resolver has decided.
    pub fn output(&self) -> &P::Output {
        match self {
            Self::Out(OutItem::One(one)) => one.output(),
            Self::Out(OutItem::Many(_)) | Self::Out(OutItem::Whole) => {
                unreachable!("a spread and the whole publish no single output")
            }
            Self::Pivot(_) => {
                unreachable!("a pivot publishes one column per value, not a single output")
            }
            Self::Delegate(_) => {
                unreachable!(
                    "a delegate publishes one column per payload item, not a single output"
                )
            }
            Self::Metadata(metadata) => metadata.output(),
        }
    }

    /// The out item this is, when it is one. A metadata group is not one,
    /// and a reader that needs an expression is told so rather than handed
    /// something it would have to classify.
    pub fn out_item(&self) -> Option<&OutItem<P>> {
        match self {
            Self::Out(item) => Some(item),
            Self::Metadata(_) | Self::Pivot(_) | Self::Delegate(_) => None,
        }
    }

    pub fn out_item_mut(&mut self) -> Option<&mut OutItem<P>> {
        match self {
            Self::Out(item) => Some(item),
            Self::Metadata(_) | Self::Pivot(_) | Self::Delegate(_) => None,
        }
    }

    /// The value this item computes, when it computes one.
    pub fn value(&self) -> Option<&DomainExpression<P>> {
        self.out_item()?.value()
    }

    pub fn value_mut(&mut self) -> Option<&mut DomainExpression<P>> {
        self.out_item_mut()?.value_mut()
    }

    /// The `as` the author wrote at this position.
    pub fn naming(&self) -> Option<&SqlIdentifier> {
        match self {
            Self::Out(OutItem::One(one)) => one.naming.as_ref(),
            // A pivot's columns are named by the values the IN witnessed;
            // a delegate's payload items carry their own namings.
            Self::Out(_) | Self::Pivot(_) | Self::Delegate(_) => None,
            Self::Metadata(metadata) => metadata.naming.as_ref(),
        }
    }
}

impl<P: Phase> From<OutItem<P>> for ReductionItem<P> {
    fn from(item: OutItem<P>) -> Self {
        Self::Out(item)
    }
}

/// A delegate selection in reduction place: `(payload) <~ [#(order)]`.
/// Surfaces values *selected* from a single row of the group (not synthesized).
/// An empty `order` is the degenerate "choose by no order" case = arbitrary.
/// Parenthesized multi-column payloads share one delegate row (coherent).
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("delegate_spec")]
pub struct DelegateSpec<P: Phase = Unresolved> {
    pub payload: Vec<OutItem<P>>,
    pub order: Vec<OrderingSpec<P>>,
}

/// ONE PUBLISHED VALUE: what computes it, the name the author baptized it
/// with, and the occurrence it publishes.
///
/// All three belong to the publication POSITION. The scalar expression
/// underneath computes a value and says nothing about publication, so a
/// value cannot carry a name into a position that publishes nothing, and
/// two publication positions cannot disagree about what one expression is
/// called.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("out_item:one")]
pub struct OneOut<P: Phase = Unresolved> {
    /// The value this position publishes. A crossed truth is one such value.
    pub expr: DomainExpression<P>,
    /// The `as` the author wrote, as written — strop and case included.
    /// `None` is unnamed, not anonymous: a reference publishes its own
    /// name and an application mints one.
    pub naming: Option<SqlIdentifier>,
    /// What this item publishes, once the resolver has decided. Phantom
    /// before resolution.
    ///
    /// PRIVATE. A port that any caller can write is a port any caller can
    /// CHOOSE, and choosing which occurrence a publication position stands
    /// at is the authority's act: the derivation that made the interface
    /// is what says which port each position got. There is no setter.
    ///
    /// Three states after resolution, not two: the position it publishes,
    /// a permanent judgment that it publishes no single one (a pivot's
    /// fan-out, or a delegate payload already standing in group position),
    /// and the interval before the publication pass has reached it. The
    /// last is not the second — a consumer that read it so would wait for
    /// an assignment that is never coming.
    output: P::Output,
}

impl<P: Phase> OneOut<P> {
    /// The column this position publishes.
    pub fn output(&self) -> &P::Output {
        &self.output
    }
}

impl<P: Phase> OneOut<P> {
    /// CROSS A PHASE BOUNDARY.
    ///
    /// The value crosses by `expr`; what the position publishes goes
    /// through the phases' OUTPUT fold, which is not an argument here and
    /// cannot be one. So a fold is never the place a position acquires a
    /// different port.
    pub fn folded<Q: Phase, F: crate::pipeline::ast_transform::AstTransform<P, Q> + ?Sized>(
        self,
        walk: &mut F,
        expr: DomainExpression<Q>,
    ) -> crate::error::Result<OneOut<Q>> {
        Ok(OneOut {
            expr,
            naming: self.naming,
            output: walk.fold_output(self.output)?,
        })
    }
}

impl<P: Phase<Output = ()>> OneOut<P> {
    /// The authored position. Nothing is paired — the phase has no port.
    pub fn authored(expr: DomainExpression<P>, naming: Option<SqlIdentifier>) -> Self {
        OneOut {
            expr,
            naming,
            output: (),
        }
    }
}

impl<P: Phase<Output = crate::relation::PortId>> OneOut<P> {
    /// THE ONE BOUND-PHASE CONSTRUCTOR, and it is the authority's. The
    /// port is the one the derivation made for THIS position.
    pub(crate) fn published(
        _authority: &crate::relation::builder::SemanticConstruction,
        expr: DomainExpression<P>,
        naming: Option<SqlIdentifier>,
        output: crate::relation::PortId,
    ) -> Self {
        OneOut {
            expr,
            naming,
            output,
        }
    }


    /// The same act over a port the authority followed one carry edge to.
    pub(crate) fn reland(
        &mut self,
        _authority: &crate::relation::builder::SemanticConstruction,
        output: crate::relation::PortId,
    ) {
        self.output = output;
    }
}

/// A publication item: one value, or a spread standing for the several it
/// covers.
///
/// A NAMED SPREAD IS UNREPRESENTABLE. The `Many` arm has no naming field,
/// and `One`'s value is a domain expression, which admits no enumerating
/// form — so no road builds an item that publishes one name across several
/// columns.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum OutItem<P: Phase = Unresolved> {
    #[lispy("out_item:one")]
    One(OneOut<P>),
    #[lispy("out_item:many")]
    Many(Spread<P>),
    /// THE WHOLE OPERAND, named rather than addressed — the publication
    /// twin of an argument row's star.
    ///
    /// A compiler-built projection that keeps everything its operand
    /// publishes means exactly this, and it is NOT the expansion of an
    /// authored glob: the operand's hygienic columns must ride through
    /// such a projection, and a column named in a select list cannot be
    /// hygienic. No authored surface builds one.
    #[lispy("out_item:whole")]
    Whole,
}

impl<P: Phase> OutItem<P> {
    /// An unnamed one-value item.
    pub fn one(one: OneOut<P>) -> Self {
        Self::One(one)
    }

    /// The value a one-value item computes. Neither of the other two
    /// computes one: a spread enumerates and the whole names, so there is
    /// no expression to hand back.
    pub fn value(&self) -> Option<&DomainExpression<P>> {
        match self {
            Self::One(one) => Some(&one.expr),
            Self::Many(_) | Self::Whole => None,
        }
    }

    pub fn value_mut(&mut self) -> Option<&mut DomainExpression<P>> {
        match self {
            Self::One(one) => Some(&mut one.expr),
            Self::Many(_) | Self::Whole => None,
        }
    }
}

/// After resolution the stamp is there to read. A spread has none of its own:
/// it published through its expansion, and the expansion is what carries the
/// occurrences — which is a PERMANENT no-single-output, not an interval, and
/// says so.
impl<P: Phase<Output = crate::relation::PortId>> OutItem<P> {
    pub fn output(&self) -> Option<crate::relation::PortId> {
        match self {
            Self::One(one) => Some(*one.output()),
            Self::Many(_) | Self::Whole => None,
        }
    }
}

/// A publication position whose name is MANDATORY.
///
/// The transform writes into a slot it names, so an unnamed item is not a
/// diagnostic — it is unbuildable. The qualifier says which live scope holds
/// the column being redefined; a self-join is where the bare name cannot say
/// it.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("named_out_item")]
pub struct NamedOutItem<P: Phase = Unresolved> {
    pub expr: DomainExpression<P>,
    pub naming: SqlIdentifier,
    pub qualifier: Option<SqlIdentifier>,
    /// The column this item writes into, once the resolver has found it.
    /// PRIVATE, for the reason [`OneOut::output`] states.
    output: P::Output,
}

impl<P: Phase> NamedOutItem<P> {
    /// The column this position writes into.
    pub fn output(&self) -> &P::Output {
        &self.output
    }
}

impl<P: Phase> NamedOutItem<P> {
    /// Cross a phase boundary. See [`OneOut::folded`].
    pub fn folded<Q: Phase, F: crate::pipeline::ast_transform::AstTransform<P, Q> + ?Sized>(
        self,
        walk: &mut F,
        expr: DomainExpression<Q>,
    ) -> crate::error::Result<NamedOutItem<Q>> {
        Ok(NamedOutItem {
            expr,
            naming: self.naming,
            qualifier: self.qualifier,
            output: walk.fold_output(self.output)?,
        })
    }
}

impl<P: Phase<Output = ()>> NamedOutItem<P> {
    /// The authored position. Nothing is paired — the phase has no port.
    pub fn authored(
        expr: DomainExpression<P>,
        naming: SqlIdentifier,
        qualifier: Option<SqlIdentifier>,
    ) -> Self {
        NamedOutItem {
            expr,
            naming,
            qualifier,
            output: (),
        }
    }
}

impl<P: Phase<Output = crate::relation::PortId>> NamedOutItem<P> {
    /// THE ONE BOUND-PHASE CONSTRUCTOR, and it is the authority's.
    pub(crate) fn published(
        _authority: &crate::relation::builder::SemanticConstruction,
        expr: DomainExpression<P>,
        naming: SqlIdentifier,
        qualifier: Option<SqlIdentifier>,
        output: crate::relation::PortId,
    ) -> Self {
        NamedOutItem {
            expr,
            naming,
            qualifier,
            output,
        }
    }

}

/// Ordering direction for ORDER BY
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum OrderDirection {
    Ascending,
    Descending,
}

/// Ordering specification
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("order_spec")]
pub struct OrderingSpec<P: Phase = Unresolved> {
    pub column: DomainExpression<P>,
    pub direction: Option<OrderDirection>,
}

/// The name an AUTHORED rename asks for: a literal, or a template expanded
/// once per matched column. Resolution spends it — a bound phase carries
/// the minted spelling, so a template cannot survive into a closed query.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum NameTarget {
    /// Literal column name: "foo"
    #[lispy("name_target:identifier")]
    Identifier(String),
    /// Column name template: :"{@}_{#}"
    #[lispy("name_target:template")]
    Template(super::operators::ColumnAlias),
}

/// Rename specification. The source ADDRESSES columns — one reference, or
/// the several a regex or glob covers — and computes nothing.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("rename")]
pub struct RenameSpec<P: Phase = Unresolved> {
    pub from: RenameSource<P>,
    pub to: P::RenameTarget,
}

/// Specification for repositioning a column. A reposition ADDRESSES a
/// column — by name or by position — and computes nothing.
#[derive(Debug, Clone, PartialEq)]
pub struct RepositionSpec<P: Phase = Unresolved> {
    pub column: super::expressions::Reference<P>,
    pub position: i32,
}

impl<P: Phase> ToLispy for RepositionSpec<P>
where
    super::expressions::Reference<P>: ToLispy,
{
    fn to_lispy(&self) -> String {
        format!(
            "(reposition-spec {} {})",
            self.column.to_lispy(),
            self.position
        )
    }
}

/// Tuple ordinal operators for LIMIT/OFFSET
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum TupleOrdinalOperator {
    LessThan,    // #<
    GreaterThan, // #>
    Exactly,     // #=
}

/// Tuple ordinal clause
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("sigma_clause:tuple_ordinal")]
pub struct TupleOrdinalClause {
    pub operator: TupleOrdinalOperator,
    pub value: i64,
    pub offset: Option<i64>,
}
