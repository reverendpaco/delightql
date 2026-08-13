// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Raw metadata transport and strict decoding for SISO introspection.

use std::collections::HashSet;

use delightql_types::introspect::{DiscoveredAttribute, DiscoveredEntity};

use crate::coprocess::SharedCoprocess;
use crate::error::{PipeError, Result};

/// The untrusted result of a metadata query. The transport preserves raw
/// strings; the decoder below owns all interpretation and validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawPipeTable {
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Vec<Vec<String>>,
}

pub(crate) trait PipeMetadataSource: Send + Sync {
    fn query_metadata(&self, sql: &str) -> Result<RawPipeTable>;
}

impl PipeMetadataSource for SharedCoprocess {
    fn query_metadata(&self, sql: &str) -> Result<RawPipeTable> {
        let (columns, rows) = self.execute_query_raw(sql)?;
        Ok(RawPipeTable { columns, rows })
    }
}

fn malformed(message: impl Into<String>) -> PipeError {
    PipeError::QueryFailed(format!("malformed pipe metadata: {}", message.into()))
}

fn required_column(columns: &[String], name: &str) -> Result<usize> {
    let matches: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.eq_ignore_ascii_case(name))
        .map(|(index, _)| index)
        .collect();
    match matches.as_slice() {
        [index] => Ok(*index),
        [] => Err(malformed(format!("missing required column '{name}'"))),
        _ => Err(malformed(format!("duplicate required column '{name}'"))),
    }
}

fn table_kind(value: &str) -> Result<i32> {
    if value.eq_ignore_ascii_case("table") || value.eq_ignore_ascii_case("base table") {
        Ok(10)
    } else if value.eq_ignore_ascii_case("view") {
        Ok(11)
    } else {
        Err(malformed(format!("unknown table_type '{value}'")))
    }
}

fn parse_nonnegative_cid(value: &str, table_name: &str) -> Result<i32> {
    let cid = value.parse::<i32>().map_err(|_| {
        malformed(format!(
            "table '{table_name}' has non-integer cid '{value}'"
        ))
    })?;
    if cid < 0 {
        return Err(malformed(format!(
            "table '{table_name}' has negative cid '{value}'"
        )));
    }
    Ok(cid)
}

fn parse_notnull(value: &str) -> Result<bool> {
    match value.trim() {
        "0" => Ok(false),
        "1" => Ok(true),
        value if value.eq_ignore_ascii_case("false") => Ok(false),
        value if value.eq_ignore_ascii_case("true") => Ok(true),
        _ => Err(malformed(format!("unknown notnull value '{value}'"))),
    }
}

fn row_field<'a>(row: &'a [String], index: usize, label: &str) -> Result<&'a str> {
    row.get(index)
        .map(String::as_str)
        .ok_or_else(|| malformed(format!("row is missing required field '{label}'")))
}

/// Decode one relation's column metadata. Both introspection roads use this
/// function, so neither road may silently invent indices or default values.
pub(crate) fn decode_relation_columns(
    raw: &RawPipeTable,
    table_name: &str,
) -> Result<Vec<DiscoveredAttribute>> {
    let cid_idx = required_column(&raw.columns, "cid")?;
    let name_idx = required_column(&raw.columns, "name")?;
    let type_idx = required_column(&raw.columns, "type")?;
    let notnull_idx = required_column(&raw.columns, "notnull")?;
    let mut seen_cids = HashSet::new();
    let mut attributes = Vec::with_capacity(raw.rows.len());

    for row in &raw.rows {
        let column_name = row_field(row, name_idx, "name")?;
        if column_name.is_empty() {
            return Err(malformed(format!(
                "table '{table_name}' has an empty column name"
            )));
        }
        let cid = parse_nonnegative_cid(row_field(row, cid_idx, "cid")?, table_name)?;
        if !seen_cids.insert(cid) {
            return Err(malformed(format!(
                "table '{table_name}' repeats cid '{cid}'"
            )));
        }
        let notnull = parse_notnull(row_field(row, notnull_idx, "notnull")?)?;
        attributes.push(DiscoveredAttribute {
            name: column_name.to_string().into(),
            data_type: row_field(row, type_idx, "type")?.to_string(),
            position: cid,
            is_nullable: !notnull,
        });
    }
    Ok(attributes)
}

/// Decode the sqlite3/duckdb/postgres-style single metadata query.
pub(crate) fn decode_single_query(raw: &RawPipeTable) -> Result<Vec<DiscoveredEntity>> {
    let table_idx = required_column(&raw.columns, "table_name")?;
    let kind_idx = required_column(&raw.columns, "table_type")?;
    let cid_idx = required_column(&raw.columns, "cid")?;
    let name_idx = required_column(&raw.columns, "col_name")?;
    let type_idx = required_column(&raw.columns, "col_type")?;
    let notnull_idx = required_column(&raw.columns, "notnull")?;

    let mut entities = Vec::new();
    let mut current_name: Option<String> = None;
    let mut current_kind = 10;
    let mut current_attributes = Vec::new();
    let mut current_cids = HashSet::new();
    let mut finished_names = HashSet::new();

    for row in &raw.rows {
        let table_name = row_field(row, table_idx, "table_name")?;
        if table_name.is_empty() {
            return Err(malformed("an entity has an empty table_name"));
        }
        let kind = table_kind(row_field(row, kind_idx, "table_type")?)?;
        let cid = parse_nonnegative_cid(row_field(row, cid_idx, "cid")?, table_name)?;
        let column_name = row_field(row, name_idx, "col_name")?;
        if column_name.is_empty() {
            return Err(malformed(format!(
                "table '{table_name}' has an empty column name"
            )));
        }
        let notnull = parse_notnull(row_field(row, notnull_idx, "notnull")?)?;

        if current_name.as_deref() != Some(table_name) {
            if let Some(name) = current_name.take() {
                finished_names.insert(name.clone());
                entities.push(DiscoveredEntity {
                    name: name.into(),
                    entity_type_id: current_kind,
                    attributes: std::mem::take(&mut current_attributes),
                });
            }
            if finished_names.contains(table_name) {
                return Err(malformed(format!(
                    "table '{table_name}' appears in more than one group"
                )));
            }
            current_name = Some(table_name.to_string());
            current_kind = kind;
            current_cids.clear();
        } else if current_kind != kind {
            return Err(malformed(format!(
                "table '{table_name}' changes table_type within one result"
            )));
        }

        if !current_cids.insert(cid) {
            return Err(malformed(format!(
                "table '{table_name}' repeats cid '{cid}'"
            )));
        }
        current_attributes.push(DiscoveredAttribute {
            name: column_name.to_string().into(),
            data_type: row_field(row, type_idx, "col_type")?.to_string(),
            position: cid,
            is_nullable: !notnull,
        });
    }

    if let Some(name) = current_name {
        entities.push(DiscoveredEntity {
            name: name.into(),
            entity_type_id: current_kind,
            attributes: current_attributes,
        });
    }
    Ok(entities)
}

/// Decode the discovery half of a two-phase introspection profile.
pub(crate) fn decode_discovery(
    raw: &RawPipeTable,
    has_type_column: bool,
) -> Result<Vec<(String, i32)>> {
    let name_idx = required_column(&raw.columns, "name")?;
    let type_idx = has_type_column.then(|| required_column(&raw.columns, "table_type"));
    let type_idx = match type_idx {
        Some(index) => Some(index?),
        None => None,
    };
    let mut names = HashSet::new();
    let mut discovered = Vec::with_capacity(raw.rows.len());
    for row in &raw.rows {
        let name = row_field(row, name_idx, "name")?;
        if name.is_empty() {
            return Err(malformed("discovery returned an empty table name"));
        }
        if !names.insert(name.to_string()) {
            return Err(malformed(format!("discovery repeats table '{name}'")));
        }
        let kind = type_idx
            .map(|index| table_kind(row_field(row, index, "table_type")?))
            .transpose()?
            .unwrap_or(10);
        discovered.push((name.to_string(), kind));
    }
    Ok(discovered)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single(columns: &[&str], rows: Vec<Vec<&str>>) -> RawPipeTable {
        RawPipeTable {
            columns: columns.iter().map(|value| (*value).to_string()).collect(),
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(str::to_string).collect())
                .collect(),
        }
    }

    #[test]
    fn single_query_rejects_invalid_metadata_instead_of_defaulting() {
        let raw = single(
            &[
                "table_name",
                "table_type",
                "cid",
                "col_name",
                "col_type",
                "notnull",
            ],
            vec![vec!["users", "table", "oops", "id", "INTEGER", "0"]],
        );
        let error = decode_single_query(&raw).expect_err("invalid cid must be loud");
        assert!(error.to_string().contains("non-integer cid"));
    }

    #[test]
    fn single_query_rejects_duplicate_positions_and_unknown_values() {
        let duplicate = single(
            &[
                "table_name",
                "table_type",
                "cid",
                "col_name",
                "col_type",
                "notnull",
            ],
            vec![
                vec!["users", "table", "0", "id", "INTEGER", "0"],
                vec!["users", "table", "0", "name", "TEXT", "0"],
            ],
        );
        assert!(decode_single_query(&duplicate)
            .expect_err("duplicate cid must be loud")
            .to_string()
            .contains("repeats cid"));

        let unknown = single(
            &[
                "table_name",
                "table_type",
                "cid",
                "col_name",
                "col_type",
                "notnull",
            ],
            vec![vec!["users", "table", "0", "id", "INTEGER", "maybe"]],
        );
        assert!(decode_single_query(&unknown)
            .expect_err("unknown notnull must be loud")
            .to_string()
            .contains("unknown notnull"));
    }

    #[test]
    fn two_phase_decoders_require_headers_and_preserve_open_type_names() {
        let discovery = single(&["name", "table_type"], vec![vec!["users", "view"]]);
        assert_eq!(
            decode_discovery(&discovery, true).unwrap(),
            vec![("users".into(), 11)]
        );
        let columns = single(
            &["cid", "name", "type", "notnull"],
            vec![vec!["0", "id", "ENGINE_SPECIFIC", "false"]],
        );
        let attrs = decode_relation_columns(&columns, "users").unwrap();
        assert_eq!(attrs[0].data_type, "ENGINE_SPECIFIC");
        assert!(attrs[0].is_nullable);

        let malformed_discovery = single(&["table_name"], vec![]);
        assert!(decode_discovery(&malformed_discovery, false)
            .expect_err("name header is required")
            .to_string()
            .contains("missing required column 'name'"));
    }
}
