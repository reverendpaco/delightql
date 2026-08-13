// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// Helper to emit validation warnings
pub(super) fn emit_validation_warning(warning: &str) {
    log::warn!("Column validation: {}", warning);
}
