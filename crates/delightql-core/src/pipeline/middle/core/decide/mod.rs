// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The deciders: one function per fact, each called from exactly one
//! constructor and, under `debug_assertions`, again by the recompute check
//! over the frozen graph.

pub(crate) mod admission;
pub(crate) mod capture;
pub(crate) mod dispatch;
pub(crate) mod document;
pub(crate) mod effect;
pub(crate) mod er;
pub(crate) mod equality;
pub(crate) mod fv;
pub(crate) mod grade;
pub(crate) mod mutation;
pub(crate) mod pivot;
pub(crate) mod recursion;
pub(crate) mod rule;
pub(crate) mod setop;
pub(crate) mod signature;
pub(crate) mod run;
