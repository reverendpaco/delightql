// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `dql target list` — the passive face of the adapter machinery
//! (JOE-EVERYBODY-DISTRIBUTION.md §3.1). Reports each known adapter
//! and where it resolves from, by walking exactly the chain
//! query-time resolution walks (`fatboy_exec::locate_fatboy`) — the
//! list and the behavior cannot disagree.
//!
//! `install` and `verify` arrive with the release pipeline
//! (deviations 3+4 in the proposal): they need published artifacts
//! and embedded digests. `list` needs neither, so it ships first.

use anyhow::Result;

use crate::fatboy_exec::{
    fatboy_name, fatboy_store_dir, locate_fatboy, FatboyLocation, FATBOY_DIR_ENV, PROFILES,
};

pub fn handle_target_list() -> Result<()> {
    let pin = std::env::var(FATBOY_DIR_ENV).ok();
    match &pin {
        Some(dir) => println!(
            "{FATBOY_DIR_ENV}: {dir} (hard pin — only this directory is searched)"
        ),
        None => {
            let store = fatboy_store_dir()
                .map(|d| d.display().to_string())
                .unwrap_or_else(|| "<no home directory>".to_string());
            println!("adapter store: {store}");
        }
    }
    println!();

    for profile in PROFILES {
        let line = match locate_fatboy(profile) {
            FatboyLocation::Pinned(p) if p.is_file() => {
                format!("installed  pinned       {}", describe(&p))
            }
            FatboyLocation::Pinned(p) => {
                format!("MISSING    pinned       {} (not there)", p.display())
            }
            FatboyLocation::Sibling(p) => {
                format!("installed  next to dql  {}", describe(&p))
            }
            FatboyLocation::Store(p) => {
                format!("installed  store        {}", describe(&p))
            }
            FatboyLocation::OnPath(p) => {
                format!("installed  PATH         {}", describe(&p))
            }
            FatboyLocation::NotFound => "not installed".to_string(),
        };
        println!("{profile:<10} {line}");
    }

    if pin.is_none() {
        println!(
            "\nTo install an adapter: dql target install <profile> --from <dir>\n\
             (or place its binary, e.g. {}, next to dql, in the store, \
             or on PATH; from source: cargo build -p delightql-postgres)",
            fatboy_name("postgres")
        );
    }
    Ok(())
}

/// `dql target install <profile> --from <dir>` — the store's write
/// path (§3.1/§3.4 of the proposal). Local-directory source only, by
/// design: no artifact host exists yet, and when one does, download
/// becomes another source in front of the same verification gate.
///
/// The gate: a release dql verifies the artifact against its burned
/// digest BEFORE anything touches the store — a mismatch installs
/// nothing. A dev dql has no digests and says so, loudly, instead of
/// pretending to verify.
pub fn handle_target_install(profile: &str, from: Option<&std::path::Path>) -> Result<()> {
    if !PROFILES.contains(&profile) {
        anyhow::bail!(
            "unknown adapter profile '{profile}' (known: {})",
            PROFILES.join(", ")
        );
    }
    let Some(dir) = from else {
        anyhow::bail!(
            "no published artifact host exists yet, so install needs a \
             local source:\n    dql target install {profile} --from <dir>\n\
             (<dir> holds the adapter binary — e.g. dist/ from \
             scripts/release-build.py, or target/release/)"
        );
    };

    let src = find_artifact(dir, profile)?;

    // The digest gate, before any write.
    let verified = match delightql_buildinfo::fatboy_digest(profile) {
        Some(expected) => {
            let actual = sha256_file(&src)
                .map_err(|e| anyhow::anyhow!("cannot read {}: {e}", src.display()))?;
            if actual != expected {
                anyhow::bail!(
                    "REFUSED: {} does not match the digest burned into this dql\n\
                     expected sha256 {expected}\n\
                     found    sha256 {actual}\n\
                     Nothing was installed.",
                    src.display()
                );
            }
            true
        }
        None => {
            eprintln!(
                "warning: this dql is a {} build and carries no adapter \
                 digests — installing UNVERIFIED",
                delightql_buildinfo::profile()
            );
            false
        }
    };

    // Copy to a temp name in the store, then rename: the store never
    // holds a half-written adapter under its real name.
    let store = fatboy_store_dir()
        .ok_or_else(|| anyhow::anyhow!("cannot determine the adapter store directory"))?;
    std::fs::create_dir_all(&store)?;
    let name = fatboy_name(profile);
    let dest = store.join(&name);
    let tmp = store.join(format!(".{name}.installing"));
    std::fs::copy(&src, &tmp)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))?;
    }
    std::fs::rename(&tmp, &dest)?;

    record_installed(&store, profile)?;

    println!(
        "installed {} -> {} ({})",
        src.display(),
        dest.display(),
        if verified { "digest verified" } else { "UNVERIFIED — dev build" }
    );

    // No lies about what a connection will now use: sibling lookup
    // outranks the store.
    if let FatboyLocation::Sibling(p) = locate_fatboy(profile) {
        println!(
            "note: a copy next to dql takes precedence over the store: {}",
            p.display()
        );
    }
    Ok(())
}

/// Find the adapter binary in a source directory: the bare name, or
/// exactly one release artifact matching this dql's version and this
/// machine's os/arch (the dist/ naming scripts/release-build.py uses).
fn find_artifact(dir: &std::path::Path, profile: &str) -> Result<std::path::PathBuf> {
    let bare = dir.join(fatboy_name(profile));
    if bare.is_file() {
        return Ok(bare);
    }
    let prefix = format!(
        "dql-fatboy-{}-{}+",
        profile,
        delightql_buildinfo::VERSION
    );
    let suffix = format!("-{}-{}", std::env::consts::OS, std::env::consts::ARCH);
    let mut hits: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
        .map_err(|e| anyhow::anyhow!("cannot read {}: {e}", dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|p| {
            p.is_file()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(&prefix) && n.ends_with(&suffix))
                    .unwrap_or(false)
        })
        .collect();
    match hits.len() {
        0 => anyhow::bail!(
            "no {profile} adapter in {}: looked for '{}' or '{prefix}…{suffix}'",
            dir.display(),
            fatboy_name(profile)
        ),
        1 => Ok(hits.remove(0)),
        _ => {
            hits.sort();
            anyhow::bail!(
                "{} {profile} adapters in {} — name one explicitly by \
                 moving it to its own directory:\n{}",
                hits.len(),
                dir.display(),
                hits.iter()
                    .map(|p| format!("  {}", p.display()))
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        }
    }
}

/// The installed-profiles record (§3.3's restore-on-upgrade nicety):
/// one profile per line in `<data>/fatboys/installed`, version-
/// independent, so a freshly upgraded dql can offer to restore the
/// set. Written on every install, deduplicated.
fn record_installed(store_version_dir: &std::path::Path, profile: &str) -> Result<()> {
    let Some(root) = store_version_dir.parent() else {
        return Ok(());
    };
    let record = root.join("installed");
    let mut profiles: Vec<String> = std::fs::read_to_string(&record)
        .unwrap_or_default()
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();
    if !profiles.iter().any(|p| p == profile) {
        profiles.push(profile.to_string());
        profiles.sort();
        std::fs::write(&record, profiles.join("\n") + "\n")?;
    }
    Ok(())
}

/// `dql target verify` — re-hash whatever the lookup chain resolves
/// against the digests burned in at release time (§3.4 of the
/// proposal: the CLI carries its own manifest). Answers "do my
/// adapters match what this dql was released with?" — a question a
/// dev build cannot answer, so it refuses rather than pretending.
pub fn handle_target_verify() -> Result<()> {
    if delightql_buildinfo::FATBOY_DIGESTS.is_none() {
        anyhow::bail!(
            "this dql is a {} build and carries no adapter digests; \
             verify applies to release builds\n\
             (identity: {})",
            delightql_buildinfo::profile(),
            delightql_buildinfo::identity()
        );
    }

    let mut mismatches = 0usize;
    for profile in PROFILES {
        let Some(expected) = delightql_buildinfo::fatboy_digest(profile) else {
            println!("{profile:<10} no digest burned for this adapter");
            continue;
        };
        match locate_fatboy(profile) {
            FatboyLocation::NotFound => {
                println!("{profile:<10} not installed (nothing to verify)");
            }
            FatboyLocation::Pinned(p) if !p.is_file() => {
                println!("{profile:<10} pinned but missing: {}", p.display());
            }
            FatboyLocation::Pinned(p)
            | FatboyLocation::Sibling(p)
            | FatboyLocation::Store(p)
            | FatboyLocation::OnPath(p) => match sha256_file(&p) {
                Ok(actual) if actual == expected => {
                    println!("{profile:<10} ok         {}", p.display());
                }
                Ok(actual) => {
                    mismatches += 1;
                    println!(
                        "{profile:<10} MISMATCH   {}\n\
                         {:<10} expected sha256 {expected}\n\
                         {:<10} found    sha256 {actual}",
                        p.display(),
                        "",
                        ""
                    );
                }
                Err(e) => {
                    mismatches += 1;
                    println!("{profile:<10} UNREADABLE {} ({e})", p.display());
                }
            },
        }
    }

    if mismatches > 0 {
        anyhow::bail!(
            "{mismatches} adapter(s) do not match this dql's burned digests"
        );
    }
    Ok(())
}

fn sha256_file(p: &std::path::Path) -> std::io::Result<String> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    let mut file = std::fs::File::open(p)?;
    std::io::copy(&mut file, &mut hasher)?;
    Ok(format!("{:x}", hasher.finalize()))
}

/// Path plus size — the honest facts we have without spawning it.
fn describe(p: &std::path::Path) -> String {
    match std::fs::metadata(p) {
        Ok(md) => format!("{} ({})", p.display(), human_size(md.len())),
        Err(_) => p.display().to_string(),
    }
}

fn human_size(bytes: u64) -> String {
    if bytes >= 1_000_000 {
        format!("{:.1} MB", bytes as f64 / 1_000_000.0)
    } else {
        format!("{} kB", bytes.div_ceil(1000))
    }
}
