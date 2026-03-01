use std::env;
use std::process::Command;

fn main() {
    // Capture version from Cargo.toml
    let version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=BUILD_VERSION={}", version);

    // Capture jj change ID (first 8 chars for brief display)
    // Commented out to speed up builds - jj operations are slow with large history
    /*
    let change_id = Command::new("jj")
        .args(["log", "-r", "@", "-T", "change_id.short()"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unavailable".to_string());

    let change_id_full = Command::new("jj")
        .args(["log", "-r", "@", "-T", "change_id"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unavailable".to_string());

    // Capture jj commit ID
    let commit_id = Command::new("jj")
        .args(["log", "-r", "@", "-T", "commit_id"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unavailable".to_string());

    // Capture jj description
    let description = Command::new("jj")
        .args(["log", "-r", "@", "-T", "description"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout)
                    .ok()
                    .map(|s| s.lines().next().unwrap_or("").to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unavailable".to_string());
    */

    let change_id = "unavailable".to_string();
    let change_id_full = "unavailable".to_string();
    let commit_id = "unavailable".to_string();
    let description = "unavailable".to_string();

    // Capture build timestamp
    let build_time = chrono::Utc::now().to_rfc3339();

    // Capture build date (for brief display)
    let build_date = chrono::Utc::now().format("%Y-%m-%d").to_string();

    // Set environment variables for use in the code
    println!("cargo:rustc-env=BUILD_CHANGE_ID_SHORT={}", change_id.trim());
    println!("cargo:rustc-env=BUILD_CHANGE_ID={}", change_id_full.trim());
    println!("cargo:rustc-env=BUILD_COMMIT_ID={}", commit_id.trim());
    println!("cargo:rustc-env=BUILD_DESCRIPTION={}", description.trim());
    println!("cargo:rustc-env=BUILD_TIME={}", build_time);
    println!("cargo:rustc-env=BUILD_DATE={}", build_date);

    // Target triple
    let target = env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=BUILD_TARGET={}", target);

    // Profile (debug/release)
    let profile = env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=BUILD_PROFILE={}", profile);

    // Capture rustc version
    let rustc_version = Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=BUILD_RUSTC_VERSION={}", rustc_version);

    // Rebuild if .jj directory changes
    // Commented out to avoid watching large .jj directory
    // println!("cargo:rerun-if-changed=../../.jj");
    println!("cargo:rerun-if-changed=build.rs");

    // Set rpath so the binary can find libduckdb.dylib at runtime (only when duckdb feature is enabled)
    #[cfg(feature = "duckdb")]
    {
        #[cfg(target_os = "macos")]
        {
            println!("cargo:rustc-link-arg=-Wl,-rpath,@executable_path/../../.mise/libs");
            println!("cargo:rustc-link-arg=-Wl,-rpath,@loader_path/../../.mise/libs");
        }

        #[cfg(target_os = "linux")]
        {
            println!("cargo:rustc-link-arg=-Wl,-rpath,/home/doeklund/ducklibs");
        }
    }
}
