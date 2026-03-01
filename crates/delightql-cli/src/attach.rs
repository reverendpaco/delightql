//! CLI/REPL attachment handling
//!
//! Routes through the session protocol via mount!() DQL pseudo-predicate.
//! mount!() handles: file validation, ATTACH DATABASE, cartridge registration,
//! introspection, namespace creation, and entity activation.

use anyhow::{anyhow, Result};

/// Process --attach CLI flags
///
/// Parses multiple attachment specifications and executes them via mount!().
/// Each spec should be in the format "path/to/db.db=namespace::path".
pub fn process_attach_flags(
    handle: &mut dyn delightql_core::api::DqlHandle,
    attach_specs: &[String],
) -> Result<()> {
    for spec in attach_specs {
        let (db_path, namespace_path) = parse_attach_spec(spec)?;
        mount_via_session(handle, &db_path, &namespace_path)?;
        eprintln!("✓ Attached {} to {}", db_path, namespace_path);
    }
    Ok(())
}

/// Parse "path=namespace" format
fn parse_attach_spec(spec: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = spec.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err(anyhow!(
            "Invalid --attach format. Expected: 'path=namespace', got: '{}'",
            spec
        ));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Handle .attach REPL dot command
///
/// Parses and executes a .attach command via mount!().
///
/// # Syntax
/// `.attach 'path/to/db.db' to "namespace::path"`
pub fn handle_attach_command(
    cmd: &str,
    handle: &mut dyn delightql_core::api::DqlHandle,
) -> Result<bool> {
    if !cmd.starts_with(".attach") {
        return Ok(false);
    }

    let cmd = cmd.trim();

    // Find the single-quoted path
    let path_start = match cmd.find('\'') {
        Some(pos) => pos + 1,
        None => {
            return Err(anyhow!(
                "Invalid .attach syntax. Expected: .attach 'path' to \"namespace\""
            ))
        }
    };

    let path_end = match cmd[path_start..].find('\'') {
        Some(pos) => path_start + pos,
        None => {
            return Err(anyhow!(
                "Invalid .attach syntax. Missing closing quote for path"
            ))
        }
    };

    let db_path = &cmd[path_start..path_end];

    // Find the double-quoted namespace
    let ns_start = match cmd[path_end..].find('"') {
        Some(pos) => path_end + pos + 1,
        None => {
            return Err(anyhow!(
                "Invalid .attach syntax. Expected: .attach 'path' to \"namespace\""
            ))
        }
    };

    let ns_end = match cmd[ns_start..].find('"') {
        Some(pos) => ns_start + pos,
        None => {
            return Err(anyhow!(
                "Invalid .attach syntax. Missing closing quote for namespace"
            ))
        }
    };

    let namespace = &cmd[ns_start..ns_end];

    mount_via_session(handle, db_path, namespace)?;
    println!("✓ Attached {} to {}", db_path, namespace);
    Ok(true)
}

/// Route through the session protocol via mount!() DQL command.
fn mount_via_session(
    handle: &mut dyn delightql_core::api::DqlHandle,
    db_path: &str,
    namespace: &str,
) -> Result<()> {
    let dql = format!("mount!(\"{}\", \"{}\")", db_path, namespace);
    let mut session = handle.session().map_err(|e| anyhow!("{}", e))?;
    crate::exec_ng::run_dql_query(&dql, &mut *session)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_attach_spec_valid() {
        let spec = "nba.db=sports::nba";
        let result = parse_attach_spec(spec);
        assert!(result.is_ok());
        let (db_path, namespace) = result.unwrap();
        assert_eq!(db_path, "nba.db");
        assert_eq!(namespace, "sports::nba");
    }

    #[test]
    fn test_parse_attach_spec_with_slash() {
        let spec = "/path/to/nba.db=sports::nba";
        let result = parse_attach_spec(spec);
        assert!(result.is_ok());
        let (db_path, namespace) = result.unwrap();
        assert_eq!(db_path, "/path/to/nba.db");
        assert_eq!(namespace, "sports::nba");
    }

    #[test]
    fn test_parse_attach_spec_deep_namespace() {
        let spec = "test.db=org::division::team::project";
        let result = parse_attach_spec(spec);
        assert!(result.is_ok());
        let (db_path, namespace) = result.unwrap();
        assert_eq!(db_path, "test.db");
        assert_eq!(namespace, "org::division::team::project");
    }

    #[test]
    fn test_parse_attach_spec_invalid() {
        let spec = "invalid_format";
        let result = parse_attach_spec(spec);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_attach_spec_multiple_equals() {
        let spec = "path=with=equals=namespace::path";
        let result = parse_attach_spec(spec);
        assert!(result.is_ok());
        let (db_path, namespace) = result.unwrap();
        assert_eq!(db_path, "path");
        assert_eq!(namespace, "with=equals=namespace::path");
    }
}
