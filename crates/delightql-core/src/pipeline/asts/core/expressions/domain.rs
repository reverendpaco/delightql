// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Domain expressions
//! DomainExpression, DomainHole

use super::super::{Phase, Unresolved};
use super::functions::FunctionApplication;
use crate::{lispy::ToLispy, ToLispy};

/// THE OPEN LEAF — the two states a callable or definition body may leave
/// unfilled, and the only two.
///
/// A hole stands at ANY value depth (`upper:(trim:(@))`), so it is a leaf of
/// the value tree rather than a wrapper around it. Only an open position —
/// a lambda, template or open-functor body, a companion cell, a sparse fill,
/// a slot list — constructs one, and the position that applies the body
/// spends it: the function pipe at build, a definition at its call-site
/// instantiation, a cover at its per-cell application, a companion cell at
/// the column it constrains. A closed position holding one refuses where the
/// leaf is met.
///
/// The landing `@` of a higher-order argument row is NOT this leaf — it is
/// `HoArgument::Landing`, structural argument information spent before a
/// closed resolved query exists. The call-site `..` is `ScalarArgument::
/// Context`. Neither can be manufactured as a value.
#[derive(Debug, Clone, Copy, PartialEq, ToLispy)]
pub enum DomainHole {
    /// `_` — the disregarded. It binds nothing.
    #[lispy("domain_hole:disregarded")]
    Disregarded,
    /// `@` — the composition input: what flows in. Lives to instantiation.
    #[lispy("domain_hole:composition_input")]
    CompositionInput,
}

#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum DomainExpression<P: Phase = Unresolved> {
    /// The one carrier that addresses a column, by name or by position.
    #[lispy("domain_expression:reference")]
    Reference(super::references::Reference<P>),
    /// THE VALUE'S ONE APPLICATION FAMILY. Everything that is not a
    /// reference is an application, and the family says of what.
    Application(FunctionApplication<P>),
}
