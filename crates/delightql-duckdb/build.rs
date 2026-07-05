// SPDX-License-Identifier: Apache-2.0
// rpath for libduckdb at runtime — same recipe as delightql-cli's
// build.rs (the duckdb crate links the shared library dynamically).
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
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
