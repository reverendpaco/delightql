// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Manifest reader — a library's `_internal` companion rules, queried as
//! relations.
//!
//! The companion rules — `imprinting`, `schema`, `constraints` and
//! `defaults` in `<library>::_internal` — are ordinary rules. Each one the
//! library declares is queried as `<library>::_internal.<companion>(*)`
//! through the whole system, and read BY COLUMN NAME from its published
//! heading. That heading must be exactly the companion's columns: a missing,
//! misspelled or extra column refuses rather than being ignored or read by
//! position. Nothing reads a companion's body.
//!
//! Every row is judged here, before any consumer touches a target: entity
//! names, materializations and extents, and the two keys — within one
//! entity, a column is declared once in `schema` and defaulted at most once
//! in `defaults`, names compared case-insensitively.

use std::collections::{BTreeSet, HashMap};

use delightql_types::DbValue;

use crate::diagnostic::{Manifest as ManifestDiagnostic, Runtime};
use crate::error::{DelightQLError, Result};

pub use super::manifest_contract::{
    ConstraintRow, DefaultRow, Extent, ImprintingRow, Materialization, OrdinalCell,
    SchemaRow,
};

/// Reject a manifest entity name that carries a `"`. The imprint DDL path
/// interpolates entity names into quoted identifiers; the declared-table
/// branch routes through the DDL generator (`ddl_pipeline::generator`, out of
/// this module), whose `write_quoted` does NOT double internal quotes, so an
/// embedded `"` would emit malformed/injected DDL there — `quote_ident` in the
/// imprint path cannot reach it. Rather than escape theater, we forbid the
/// character at the source (a triple-quoted DQL literal `"""a"b"""` is the only
/// way one reaches here). Pinned by
/// `manifest::tests::entity_name_rejects_embedded_quote`.
fn validate_entity_name(name: &str) -> Result<()> {
    if name.contains('"') {
        return Err(DelightQLError::from(ManifestDiagnostic::EntityName {
            message: format!(
                "imprint entity name '{}' contains a '\"' — entity names may not \
                 contain double quotes",
                name
            ),
        }));
    }
    Ok(())
}

/// The companion rules the manifest reads, each with exactly its columns.
pub(crate) const COMPANIONS: [(&str, &[&str]); 4] = [
    ("imprinting", &["entity", "materialization", "extent"]),
    ("schema", &["entity", "name", "type", "ordinal"]),
    ("constraints", &["entity", "column", "constraint", "constraint_name"]),
    ("defaults", &["entity", "column", "default_val"]),
];

/// One companion rule, queried: its published heading and its rows.
pub(crate) struct CompanionAnswer {
    pub(crate) heading: Vec<Option<String>>,
    pub(crate) rows: Vec<Vec<DbValue>>,
}

/// A library's manifest, read and judged.
pub struct ManifestRows {
    /// The namespace the companion rules are declared in: where their
    /// constraint and default cells stand.
    namespace: String,
    imprinting: Option<Vec<ImprintingRow>>,
    schema: Vec<(String, SchemaRow)>,
    constraints: Vec<(String, ConstraintRow)>,
    defaults: Vec<(String, DefaultRow)>,
}

impl ManifestRows {
    /// Read a library's manifest through the whole system. `Ok(None)`: the
    /// library declares no `_internal` namespace. The caller holds no
    /// catalog lock — the queries take it.
    pub(crate) fn read(
        system: &crate::system::DelightQLSystem,
        library_fq: &str,
    ) -> Result<Option<ManifestRows>> {
        let Some(declared) = system.companion_rules_of(library_fq)? else {
            return Ok(None);
        };
        let mut answers = HashMap::new();
        for (companion, _) in COMPANIONS {
            if declared.iter().any(|name| name == companion) {
                let source = format!("{library_fq}::_internal.{companion}(*)");
                answers.insert(companion, system.query_in_system(&source)?);
            }
        }
        Self::judge(library_fq, answers).map(Some)
    }

    /// The namespace the companion rules are declared in.
    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Judge the companions a library declared, as queried.
    pub(crate) fn judge(
        library_fq: &str,
        mut answers: HashMap<&'static str, CompanionAnswer>,
    ) -> Result<ManifestRows> {
        let imprinting_present = answers.contains_key("imprinting");
        let mut read = |companion: &'static str| -> Result<Vec<Vec<DbValue>>> {
            let Some(answer) = answers.remove(companion) else {
                return Ok(Vec::new());
            };
            let expected = COMPANIONS
                .iter()
                .find(|(name, _)| *name == companion)
                .map(|(_, columns)| *columns)
                .unwrap_or(&[]);
            let order = columns_by_name(library_fq, companion, expected, &answer.heading)?;
            Ok(answer
                .rows
                .into_iter()
                .map(|row| order.iter().map(|&at| row[at].clone()).collect())
                .collect())
        };
        let imprinting_rows = read("imprinting")?;
        let schema_rows = read("schema")?;
        let constraint_rows = read("constraints")?;
        let default_rows = read("defaults")?;

        let imprinting = if imprinting_present {
            let mut rows = Vec::with_capacity(imprinting_rows.len());
            for row in imprinting_rows {
                let entity = entity_of("imprinting", &row[0])?;
                validate_entity_name(&entity)?;
                rows.push(ImprintingRow {
                    entity,
                    materialization: Materialization::parse(&text("imprinting", "materialization", &row[1])?)?,
                    extent: Extent::parse(&text("imprinting", "extent", &row[2])?)?,
                });
            }
            Some(rows)
        } else {
            None
        };

        let mut schema = Vec::with_capacity(schema_rows.len());
        for row in schema_rows {
            schema.push((
                entity_of("schema", &row[0])?,
                SchemaRow {
                    name: text("schema", "name", &row[1])?,
                    col_type: text("schema", "type", &row[2])?,
                    ordinal: ordinal_cell(&row[3]),
                },
            ));
        }
        let mut constraints = Vec::with_capacity(constraint_rows.len());
        for row in constraint_rows {
            constraints.push((
                entity_of("constraints", &row[0])?,
                ConstraintRow {
                    column: text("constraints", "column", &row[1])?,
                    constraint: text("constraints", "constraint", &row[2])?,
                    constraint_name: text("constraints", "constraint_name", &row[3])?,
                },
            ));
        }
        let mut defaults = Vec::with_capacity(default_rows.len());
        for row in default_rows {
            defaults.push((
                entity_of("defaults", &row[0])?,
                DefaultRow {
                    column: text("defaults", "column", &row[1])?,
                    default_val: default_value(&row[2]),
                },
            ));
        }

        refuse_repeats(&schema, "schema", |row| &row.name, "declares")?;
        refuse_repeats(&defaults, "defaults", |row| &row.column, "gives a default to")?;

        Ok(ManifestRows {
            namespace: format!("{library_fq}::_internal"),
            imprinting,
            schema,
            constraints,
            defaults,
        })
    }

    /// The `imprinting` rows; empty when the companion is absent.
    pub fn imprinting(&self) -> &[ImprintingRow] {
        self.imprinting.as_deref().unwrap_or(&[])
    }

    /// Every entity with `schema` rows — the discovery road when the
    /// library declares no `imprinting` rule.
    pub fn schema_entities(&self) -> Result<Vec<String>> {
        let names: BTreeSet<&String> = self.schema.iter().map(|(entity, _)| entity).collect();
        for name in &names {
            validate_entity_name(name)?;
        }
        Ok(names.into_iter().cloned().collect())
    }

    pub fn schema(&self, entity: &str) -> Vec<SchemaRow> {
        of_entity(&self.schema, entity)
    }

    pub fn constraints(&self, entity: &str) -> Vec<ConstraintRow> {
        of_entity(&self.constraints, entity)
    }

    pub fn defaults(&self, entity: &str) -> Vec<DefaultRow> {
        of_entity(&self.defaults, entity)
    }
}

fn of_entity<T: Clone>(rows: &[(String, T)], entity: &str) -> Vec<T> {
    rows.iter()
        .filter(|(owner, _)| owner == entity)
        .map(|(_, row)| row.clone())
        .collect()
}

/// Each expected column's position in the published heading. The heading
/// must be exactly the companion's columns, in any order.
fn columns_by_name(
    library_fq: &str,
    companion: &str,
    expected: &[&str],
    heading: &[Option<String>],
) -> Result<Vec<usize>> {
    let refuse = |detail: String| {
        DelightQLError::from(ManifestDiagnostic::CompanionHeading {
            message: format!(
                "companion rule '{library_fq}::_internal.{companion}' {detail}; its head must \
                 name exactly ({})",
                expected.join(", ")
            ),
        })
    };
    let mut at: Vec<Option<usize>> = vec![None; expected.len()];
    for (position, name) in heading.iter().enumerate() {
        let Some(name) = name else {
            return Err(refuse(format!(
                "publishes position {} with no name",
                position + 1
            )));
        };
        match expected
            .iter()
            .position(|column| column.eq_ignore_ascii_case(name))
        {
            Some(index) if at[index].is_none() => at[index] = Some(position),
            Some(_) => return Err(refuse(format!("publishes '{name}' twice"))),
            None => {
                return Err(refuse(format!(
                    "publishes '{name}', which is not one of its columns"
                )))
            }
        }
    }
    let missing: Vec<&str> = expected
        .iter()
        .zip(&at)
        .filter(|(_, found)| found.is_none())
        .map(|(column, _)| *column)
        .collect();
    if !missing.is_empty() {
        return Err(refuse(format!(
            "publishes no '{}' column",
            missing.join("', '")
        )));
    }
    Ok(at.into_iter().flatten().collect())
}

/// Within one entity, a column appears once.
fn refuse_repeats<T>(
    rows: &[(String, T)],
    companion: &str,
    column: impl Fn(&T) -> &String,
    verb: &str,
) -> Result<()> {
    let mut seen: HashMap<(String, String), &String> = HashMap::new();
    for (entity, row) in rows {
        let name = column(row);
        if let Some(first) = seen.insert((entity.clone(), name.to_ascii_lowercase()), name) {
            let spelled = if first == name {
                format!("'{name}'")
            } else {
                format!("'{first}' and '{name}'")
            };
            return Err(DelightQLError::from(ManifestDiagnostic::DuplicateColumn {
                message: format!(
                    "imprint!() entity '{entity}': '{companion}' {verb} the column {spelled} \
                     twice (column names compare case-insensitively)"
                ),
            }));
        }
    }
    Ok(())
}

fn entity_of(companion: &str, value: &DbValue) -> Result<String> {
    match value {
        DbValue::Text(entity) => Ok(entity.clone()),
        other => Err(DelightQLError::from(ManifestDiagnostic::CompanionKey {
            message: format!(
                "companion rule '{companion}' has a row whose entity is {}, not a name",
                other.storage_class()
            ),
        })),
    }
}

fn text(companion: &str, column: &str, value: &DbValue) -> Result<String> {
    match value {
        DbValue::Text(text) => Ok(text.clone()),
        other => Err(DelightQLError::from(Runtime::General {
            message: format!(
                "companion rule '{companion}' has a row whose '{column}' is {}, not text",
                other.storage_class()
            ),
            details: "Companion cell is not text".to_string(),
        })),
    }
}

/// The ordinal as written; the correspondence refuses anything that is not
/// an integer, naming the column.
fn ordinal_cell(value: &DbValue) -> OrdinalCell {
    match value {
        DbValue::Integer(value) => OrdinalCell::Integer(*value),
        DbValue::Null => OrdinalCell::Other("NULL".to_string()),
        DbValue::Real(value) => OrdinalCell::Other(value.to_string()),
        DbValue::Text(value) => OrdinalCell::Other(value.clone()),
        DbValue::Blob(_) => OrdinalCell::Other("a BLOB".to_string()),
    }
}

/// A default cell holds a plain value or a sigil expression; a number is
/// its own spelling.
fn default_value(value: &DbValue) -> String {
    match value {
        DbValue::Text(text) => text.clone(),
        DbValue::Integer(value) => value.to_string(),
        DbValue::Real(value) => value.to_string(),
        other => format!("{:?}", other),
    }
}

#[cfg(test)]
mod tests {
    //! Manifest-read validation. The imprinting()
    //! materialization/extent columns and entity names are validated the moment
    //! they leave the bootstrap DB, so a typo can never silently pick the wrong
    //! materialization/extent or inject an unescaped identifier downstream.
    use super::*;

    #[test]
    fn materialization_parses_known() {
        assert_eq!(
            Materialization::parse("table").unwrap(),
            Materialization::Table
        );
        assert_eq!(
            Materialization::parse("view").unwrap(),
            Materialization::View
        );
    }

    #[test]
    fn materialization_rejects_typo() {
        // A plain String comparison lets "veiw" fall through to a table.
        let err = Materialization::parse("veiw").unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://imprint/manifest/materialization"
        );
        assert!(err.to_string().contains("veiw"), "{}", err);
    }

    #[test]
    fn extent_parses_known() {
        assert_eq!(Extent::parse("permanent").unwrap(), Extent::Permanent);
        assert_eq!(Extent::parse("temporary").unwrap(), Extent::Temporary);
    }

    #[test]
    fn extent_rejects_typo() {
        // Comparing only against "temporary" makes "temp" mean permanent.
        let err = Extent::parse("temp").unwrap_err();
        assert_eq!(err.error_uri(), "delightql-error://imprint/manifest/extent");
        assert!(err.to_string().contains("temp"), "{}", err);
    }

    #[test]
    fn entity_name_accepts_plain() {
        assert!(validate_entity_name("seniors").is_ok());
    }

    #[test]
    fn entity_name_rejects_embedded_quote() {
        // Reachable via a triple-quoted DQL literal `"""a"b"""` → strip → a"b.
        let err = validate_entity_name("a\"b").unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://imprint/manifest/entity_name"
        );
    }
}
