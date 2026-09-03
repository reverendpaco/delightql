// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Explicit, slow architecture verification.
//!
//! This package is a workspace member but not a default member. Its tests may
//! invoke child compilers and therefore run only through the named
//! architecture-validation lane, never as part of `delightql-core`'s ordinary
//! unit suite.

#[cfg(test)]
mod construction_fence;
