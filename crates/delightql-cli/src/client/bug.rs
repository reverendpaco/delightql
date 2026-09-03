// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `.bug`: the session files now, plus the resources a replay needs, in
//! one tarball. Nothing here is a second record — the description is an
//! incident row, the log and the context are the exit's own projections
//! taken early, and `repl.sqlite` is the client database serialized.

use std::path::{Path, PathBuf};

use delightql_core::api::DqlHandle;

use super::database::ClientDatabase;
use super::exit::{snapshot, SessionFiles};
use super::incident::{hierarchy, Incident, IncidentKind};

/// Where the report landed and what it carries.
#[derive(Debug, Clone)]
pub struct BugReport {
    pub archive: PathBuf,
    pub files: SessionFiles,
    /// Every database file shipped, with its path inside the archive.
    pub databases: Vec<(PathBuf, String)>,
    /// Every consulted DDL file shipped, with its path inside the archive.
    pub ddl_files: Vec<(PathBuf, String)>,
}

/// Write `bug-<MS>.tgz` beside the session files. `primary` is the
/// user's database when the session has one; the mounted resources come
/// from `sys::cartridges` through the handle.
pub fn write_bug_report(
    db: &ClientDatabase,
    handle: &mut dyn DqlHandle,
    description: Option<&str>,
    primary: Option<&Path>,
) -> anyhow::Result<BugReport> {
    if let Some(words) = description.map(str::trim).filter(|w| !w.is_empty()) {
        db.record_incident(Incident::plain(
            IncidentKind::Info,
            "dot_command",
            hierarchy::REPORT_DESCRIPTION,
            words.to_string(),
        ));
    }

    let (ddl_files, db_files) = mounted_resources(handle, primary);

    let files = snapshot(db, handle)
        .ok_or_else(|| anyhow::anyhow!("the session files could not be written"))?;
    let image = db.serialize()?;

    let prefix = format!("bug-{}", files.stamp);
    let archive = files.directory.join(format!("{prefix}.tgz"));
    let file = std::fs::File::create(&archive)?;
    let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    let mut tar = tar::Builder::new(enc);

    for path in [&files.error_log, &files.context, &files.replay_script] {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        tar.append_path_with_name(path, format!("{prefix}/{name}"))?;
    }
    append_bytes(&mut tar, &format!("{prefix}/repl.sqlite"), &image)?;

    let mut databases = Vec::new();
    for path in &db_files {
        let name = archive_name(path, "db");
        let inside = format!("db/{name}");
        tar.append_path_with_name(path, format!("{prefix}/{inside}"))?;
        databases.push((path.clone(), inside));
    }
    let mut shipped_ddl = Vec::new();
    for path in &ddl_files {
        let name = archive_name(path, "dql");
        let inside = format!("ddl/{name}");
        tar.append_path_with_name(path, format!("{prefix}/{inside}"))?;
        shipped_ddl.push((path.clone(), inside));
    }

    tar.into_inner()?.finish()?;
    Ok(BugReport {
        archive,
        files,
        databases,
        ddl_files: shipped_ddl,
    })
}

fn append_bytes<W: std::io::Write>(
    tar: &mut tar::Builder<W>,
    name: &str,
    bytes: &[u8],
) -> std::io::Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, name, bytes)
}

fn archive_name(path: &Path, fallback_ext: &str) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| format!("unnamed.{fallback_ext}"))
}

/// The files behind the session's cartridges: DDL sources and database
/// files, by the same query the old report used. Resource kinds are the
/// cartridge enum's: 1 = DDL file, 3 = database.
fn mounted_resources(handle: &mut dyn DqlHandle, primary: Option<&Path>) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let mut ddl_files: Vec<PathBuf> = Vec::new();
    let mut db_files: Vec<PathBuf> = Vec::new();
    if let Some(p) = primary {
        if p.exists() {
            db_files.push(p.to_path_buf());
        }
    }
    let rows = handle.session().ok().and_then(|mut session| {
        crate::exec_ng::run_dql_query(
            "sys::cartridges.cartridge(*) |> (source_uri, source_type_enum)",
            &mut *session,
        )
        .ok()
    });
    let Some(rows) = rows else {
        return (ddl_files, db_files);
    };
    let uri_col = rows.columns.iter().position(|c| c == "source_uri").unwrap_or(0);
    let kind_col = rows
        .columns
        .iter()
        .position(|c| c == "source_type_enum")
        .unwrap_or(1);
    for row in &rows.rows {
        let (Some(uri), Some(kind)) = (row.get(uri_col), row.get(kind_col)) else {
            continue;
        };
        let Some(path) = file_path_of(uri) else {
            continue;
        };
        match kind.as_str() {
            "1" if !ddl_files.contains(&path) => ddl_files.push(path),
            "3" if !db_files.contains(&path) => db_files.push(path),
            _ => {}
        }
    }
    (ddl_files, db_files)
}

/// A cartridge's source URI as a filesystem path, when it names one:
/// `file://<path>` and `delightql-siso://<profile>/<path>`. Bootstrap,
/// catalog, sys and embedded sources have no file.
fn file_path_of(uri: &str) -> Option<PathBuf> {
    let path = if let Some(p) = uri.strip_prefix("file://") {
        PathBuf::from(p)
    } else if let Some(rest) = uri.strip_prefix("delightql-siso://") {
        let (_, p) = rest.split_once('/')?;
        if p.is_empty() {
            return None;
        }
        PathBuf::from(p)
    } else {
        return None;
    };
    path.exists().then_some(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cartridge_uris_resolve_to_files_only_when_they_name_one() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("x.db");
        std::fs::write(&real, b"").unwrap();
        assert_eq!(file_path_of(&format!("file://{}", real.display())), Some(real.clone()));
        assert_eq!(
            file_path_of(&format!("delightql-siso://prof/{}", real.display())),
            Some(real.clone())
        );
        assert_eq!(file_path_of("file:///nonexistent/never.db"), None);
        assert_eq!(file_path_of("bootstrap://sys"), None);
        assert_eq!(file_path_of("delightql-siso://prof/"), None);
    }
}
