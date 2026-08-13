// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Every burned man page stays inside the scrubber's house dialect.
//!
//! `dql man` scrubs ALL pages at load time, so one drifted page refuses
//! every page's rendering, not just its own — and authoring happens in
//! assets/man/man1 where nothing else checks the dialect until serve
//! time. This pins the property at the source.

#[test]
fn every_man_page_scrubs_clean() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../assets/man/man1");
    let mut checked = 0;
    for entry in std::fs::read_dir(&dir).expect("man page directory") {
        let path = entry.expect("directory entry").path();
        if path.extension().is_some_and(|e| e == "1") {
            let troff = std::fs::read_to_string(&path).expect("read page");
            delightql_cli::man_scrub::scrub(&troff)
                .unwrap_or_else(|e| panic!("{} outside house dialect: {e}", path.display()));
            checked += 1;
        }
    }
    assert!(checked >= 10, "suspiciously few man pages: {checked}");
}
