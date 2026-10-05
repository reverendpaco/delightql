// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE IMPRINT CORRESPONDENCE — which source value reaches which schema
//! column, and the physical order the created table takes, decided once.
//!
//! A companion `schema` relation is a set of typed column declarations; its
//! row order carries no meaning. A source value therefore reaches the schema
//! column of the SAME NAME, never the column standing at the same position.
//! The created table's physical order is the schema's `ordinal` column and
//! nothing else: every row states it. Both facts are read off this one
//! value: the `CREATE TABLE` column order and the `INSERT` column list cannot
//! be assembled from independently chosen positions.

use crate::ddl::manifest_contract::{OrdinalCell, SchemaRow};
use crate::diagnostic::Manifest;
use crate::error::{DelightQLError, Result};
use delightql_types::SqlIdentifier;

/// One entity's decided correspondence.
///
/// The fields are private: a consumer reads the physical order and the
/// insert routing off the same judgment, and nothing constructs one from a
/// column list beside a schema.
#[derive(Debug)]
pub struct Correspondence {
    physical: Vec<SchemaRow>,
    /// The source heading, in the source's own order, when the entity is
    /// populated from a body. Each name is a schema column by construction.
    routed: Option<Vec<String>>,
}

impl Correspondence {
    /// Decide the correspondence for one entity from its schema rows and, when
    /// a body populates it, the body's published heading.
    pub fn decide(
        entity: &str,
        schema: Vec<SchemaRow>,
        source: Option<&[Option<String>]>,
    ) -> Result<Self> {
        let routed = match source {
            None => None,
            Some(heading) => Some(route_by_name(entity, &schema, heading)?),
        };
        let physical = ordinals(entity, &schema)?;
        Ok(Correspondence { physical, routed })
    }

    /// The schema rows in the created table's physical order.
    pub fn physical(&self) -> &[SchemaRow] {
        &self.physical
    }

    /// The column list an `INSERT` names, in the source's heading order —
    /// each the schema column the same-named source position feeds.
    pub fn insert_columns(&self) -> Option<&[String]> {
        self.routed.as_deref()
    }
}

/// Every source position names exactly one schema column, and every schema
/// column is named by one source position. The refusals name the column.
fn route_by_name(
    entity: &str,
    schema: &[SchemaRow],
    heading: &[Option<String>],
) -> Result<Vec<String>> {
    let mut routed: Vec<String> = Vec::with_capacity(heading.len());
    for (position, name) in heading.iter().enumerate() {
        let Some(name) = name else {
            return Err(DelightQLError::from(Manifest::SourceColumn {
                message: format!(
                    "imprint!() entity '{entity}': the body's position {} publishes no name, \
                     so no schema column can receive it — name every published column",
                    position + 1
                ),
            }));
        };
        let matching: Vec<&SchemaRow> = schema
            .iter()
            .filter(|row| same_column(&row.name, name))
            .collect();
        match matching.as_slice() {
            [row] => {
                if routed.iter().any(|routed| same_column(routed, &row.name)) {
                    return Err(DelightQLError::from(Manifest::SourceColumn {
                        message: format!(
                            "imprint!() entity '{entity}': the body publishes '{name}' more \
                             than once, so the schema column receives two values"
                        ),
                    }));
                }
                routed.push(row.name.clone());
            }
            [] => {
                return Err(DelightQLError::from(Manifest::SourceColumn {
                    message: format!(
                        "imprint!() entity '{entity}': the body publishes '{name}', which the \
                         schema does not declare — declare it in schema(\"{entity}\"), or drop \
                         it from the body"
                    ),
                }))
            }
            _ => {
                return Err(DelightQLError::from(Manifest::SourceColumn {
                    message: format!(
                        "imprint!() entity '{entity}': the schema declares '{name}' more than \
                         once, so the body's value does not say which column it is for"
                    ),
                }))
            }
        }
    }
    if let Some(unfed) = schema
        .iter()
        .find(|row| !routed.iter().any(|routed| same_column(routed, &row.name)))
    {
        return Err(DelightQLError::from(Manifest::SchemaColumn {
            message: format!(
                "imprint!() entity '{entity}': schema column '{}' receives no value — the \
                 body publishes [{}]; publish '{}' or drop it from the schema",
                unfed.name,
                routed.join(", "),
                unfed.name
            ),
        }));
    }
    Ok(routed)
}

/// The physical order the schema's `ordinal` cells state: the rows in
/// ordinal order when the cells form exactly the permutation 1..n, a refusal
/// naming the column otherwise.
fn ordinals(entity: &str, schema: &[SchemaRow]) -> Result<Vec<SchemaRow>> {
    let refuse = |message: String| {
        DelightQLError::from(Manifest::Ordinal {
            message: format!("imprint!() entity '{entity}': {message}"),
        })
    };
    let n = schema.len();
    let mut by_position: Vec<Option<&SchemaRow>> = vec![None; n];
    for row in schema {
        let position = match &row.ordinal {
            OrdinalCell::Other(text) if text == "NULL" => {
                return Err(refuse(format!(
                    "column '{}' has no ordinal — every schema row states its column's \
                     ordinal",
                    row.name
                )))
            }
            OrdinalCell::Other(text) => {
                return Err(refuse(format!(
                    "column '{}' has ordinal {text}, which is not an integer",
                    row.name
                )))
            }
            OrdinalCell::Integer(value) => *value,
        };
        if position < 1 || position > n as i64 {
            return Err(refuse(format!(
                "column '{}' has ordinal {position}, outside 1..{n}",
                row.name
            )));
        }
        let slot = &mut by_position[(position - 1) as usize];
        if let Some(taken) = slot {
            return Err(refuse(format!(
                "columns '{}' and '{}' both have ordinal {position}",
                taken.name, row.name
            )));
        }
        *slot = Some(row);
    }
    // Every row placed at a distinct position in 1..n fills every position.
    Ok(by_position
        .into_iter()
        .map(|row| {
            row.expect("n distinct positions in 1..n fill every slot")
                .clone()
        })
        .collect())
}

/// Column names agree by the identifier law for unstropped spellings.
fn same_column(a: &str, b: &str) -> bool {
    SqlIdentifier::str_eq(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(name: &str, ordinal: i64) -> SchemaRow {
        SchemaRow {
            name: name.to_string(),
            col_type: "INTEGER".to_string(),
            ordinal: OrdinalCell::Integer(ordinal),
        }
    }

    fn row_with(name: &str, ordinal: OrdinalCell) -> SchemaRow {
        SchemaRow {
            name: name.to_string(),
            col_type: "INTEGER".to_string(),
            ordinal,
        }
    }

    fn names(rows: &[SchemaRow]) -> Vec<&str> {
        rows.iter().map(|row| row.name.as_str()).collect()
    }

    fn heading(names: &[&str]) -> Vec<Option<String>> {
        names.iter().map(|name| Some(name.to_string())).collect()
    }

    #[test]
    fn values_route_by_name_and_the_ordinal_decides_the_physical_order() {
        let decided = Correspondence::decide(
            "data",
            vec![row("v", 2), row("id", 1)],
            Some(&heading(&["v", "id"])),
        )
        .unwrap();
        assert_eq!(names(decided.physical()), ["id", "v"]);
        assert_eq!(decided.insert_columns().unwrap(), ["v", "id"]);
    }

    #[test]
    fn an_ordinal_permutation_fixes_the_physical_order() {
        let decided = Correspondence::decide(
            "data",
            vec![row("v", 1), row("id", 2)],
            Some(&heading(&["id", "v"])),
        )
        .unwrap();
        assert_eq!(names(decided.physical()), ["v", "id"]);
        assert_eq!(decided.insert_columns().unwrap(), ["id", "v"]);
    }

    #[test]
    fn the_declaration_row_order_never_decides() {
        let decided = Correspondence::decide("t", vec![row("b", 2), row("a", 1)], None).unwrap();
        assert_eq!(names(decided.physical()), ["a", "b"]);
        assert!(decided.insert_columns().is_none());
    }

    fn refuses(schema: Vec<SchemaRow>, source: Option<&[Option<String>]>) -> String {
        let error = Correspondence::decide("data", schema, source).unwrap_err();
        format!("{} {}", error.error_uri(), error)
    }

    #[test]
    fn a_missing_ordinal_refuses_by_column() {
        let missing = refuses(
            vec![row("a", 1), row_with("b", OrdinalCell::Other("NULL".to_string()))],
            None,
        );
        assert!(
            missing.contains("imprint/manifest/ordinal") && missing.contains("'b'"),
            "{missing}"
        );
    }

    #[test]
    fn name_set_disagreements_refuse_by_column() {
        let extra = refuses(vec![row("id", 1)], Some(&heading(&["id", "v"])));
        assert!(extra.contains("imprint/manifest/source_column"), "{extra}");
        assert!(extra.contains("'v'"), "{extra}");
        let missing = refuses(vec![row("id", 1), row("v", 2)], Some(&heading(&["id"])));
        assert!(
            missing.contains("imprint/manifest/schema_column"),
            "{missing}"
        );
        assert!(missing.contains("'v'"), "{missing}");
        let unnamed = refuses(vec![row("id", 1)], Some(&[None]));
        assert!(
            unnamed.contains("imprint/manifest/source_column"),
            "{unnamed}"
        );
    }

    #[test]
    fn invalid_ordinals_refuse_by_column() {
        let dup = refuses(vec![row("a", 1), row("b", 1)], None);
        assert!(
            dup.contains("imprint/manifest/ordinal") && dup.contains("'b'"),
            "{dup}"
        );
        let gap = refuses(vec![row("a", 1), row("b", 3)], None);
        assert!(gap.contains("outside 1..2"), "{gap}");
        let text = refuses(
            vec![row_with("a", OrdinalCell::Other("first".to_string()))],
            None,
        );
        assert!(text.contains("not an integer"), "{text}");
    }
}
