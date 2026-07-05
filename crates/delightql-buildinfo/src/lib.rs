// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Build identity for DelightQL binaries.
//!
//! Depended on ONLY by binary crates (`dql`, `dql-test-ball-runner`) — never
//! by a library crate. There is deliberately no build.rs: `option_env!` is
//! tracked by rustc's dep-info, so when `DQL_BUILD_IDENT` is unset (every
//! dev build) nothing ever rebuilds, and when a release/CI wrapper sets it
//! (`DQL_BUILD_IDENT="<change_id>+<commit_id>"`, computed from `jj log -r @`,
//! never from the lagging colocated git HEAD) only this crate and the
//! binaries recompile.
//!
//! Identity contract (TEST-ARCHITECTURE.md §11): dev builds report "dev" —
//! in monorepo mode, build-from-source IS the attribution. Supplied mode
//! (CI, releases, conformance consumers) must refuse binaries that report
//! "dev".

/// Crate version (workspace-wide).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Burned identity, if any: `<change_id>+<commit_id>` from the release/CI
/// wrapper. `None` for dev builds.
pub const IDENT: Option<&str> = option_env!("DQL_BUILD_IDENT");

/// The identity string: the burned value, or "dev".
pub fn identity() -> &'static str {
    IDENT.unwrap_or("dev")
}

pub fn profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

/// Human-oriented version line, e.g. `0.1.0 (dev, debug, linux/aarch64)`.
pub fn human() -> String {
    format!(
        "{} ({}, {}, {}/{})",
        VERSION,
        identity(),
        profile(),
        std::env::consts::OS,
        std::env::consts::ARCH
    )
}

/// `human()` as a `&'static str` — what clap's `version =` attribute
/// wants without enabling clap's `string` feature.
pub fn human_static() -> &'static str {
    static S: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    S.get_or_init(human)
}

/// Machine-readable identity. Hand-rolled JSON: the values are
/// compile-time constants and platform tokens (no quoting hazards), and
/// this crate stays dependency-free by design.
pub fn json() -> String {
    format!(
        "{{\"version\":\"{}\",\"identity\":\"{}\",\"profile\":\"{}\",\"os\":\"{}\",\"arch\":\"{}\"}}",
        VERSION,
        identity(),
        profile(),
        std::env::consts::OS,
        std::env::consts::ARCH
    )
}
