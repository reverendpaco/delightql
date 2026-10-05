// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The middle end: one statement elaborated into a frozen core
//! graph in which every semantic fact of the fragment is decided once.
//!
//! It is the query entrance of every road: a statement it does not cover
//! refuses, and is never handed to the predecessor's middle. `facade` is
//! the only file under this module that names anything else in the crate; every
//! other file reaches retained code through it.

pub(crate) mod api;
mod core;
mod elaborate;
mod facade;
mod facts;
mod select;
mod realize;
