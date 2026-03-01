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
