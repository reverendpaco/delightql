// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
pub struct BuildInfo {
    pub version: &'static str,
    pub change_id_short: &'static str,
    pub change_id: &'static str,
    pub commit_id: &'static str,
    pub description: &'static str,
    pub build_time: &'static str,
    pub build_date: &'static str,
    pub target: &'static str,
    pub profile: &'static str,
    pub rustc_version: &'static str,
}

pub const BUILD_INFO: BuildInfo = BuildInfo {
    version: match option_env!("BUILD_VERSION") {
        Some(v) => v,
        None => "unknown",
    },
    change_id_short: match option_env!("BUILD_CHANGE_ID_SHORT") {
        Some(v) => v,
        None => "unavailable",
    },
    change_id: match option_env!("BUILD_CHANGE_ID") {
        Some(v) => v,
        None => "unavailable",
    },
    commit_id: match option_env!("BUILD_COMMIT_ID") {
        Some(v) => v,
        None => "unavailable",
    },
    description: match option_env!("BUILD_DESCRIPTION") {
        Some(v) => v,
        None => "unavailable",
    },
    build_time: match option_env!("BUILD_TIME") {
        Some(v) => v,
        None => "unavailable",
    },
    build_date: match option_env!("BUILD_DATE") {
        Some(v) => v,
        None => "unavailable",
    },
    target: match option_env!("BUILD_TARGET") {
        Some(v) => v,
        None => "unknown",
    },
    profile: match option_env!("BUILD_PROFILE") {
        Some(v) => v,
        None => "unknown",
    },
    rustc_version: match option_env!("BUILD_RUSTC_VERSION") {
        Some(v) => v,
        None => "unknown",
    },
};

/// sha256 of this binary's own executable file. Distinguishes two builds
/// whose version strings are identical — the identity contract reports
/// "dev" for every from-source build, so without this a swapped dev
/// binary is invisible to `dql version`. Computed only on demand (the
/// version command), never at startup.
pub fn binary_sha256() -> Option<String> {
    use sha2::{Digest, Sha256};
    let exe = std::env::current_exe().ok()?;
    let bytes = std::fs::read(exe).ok()?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Some(format!("{:x}", hasher.finalize()))
}

/// Get version information as a formatted string
pub fn get_version_info() -> String {
    if BUILD_INFO.change_id_short != "unavailable" {
        format!(
            "delightql {} ({} {})",
            BUILD_INFO.version, BUILD_INFO.change_id_short, BUILD_INFO.build_date
        )
    } else {
        format!("delightql {} (build info unavailable)", BUILD_INFO.version)
    }
}
