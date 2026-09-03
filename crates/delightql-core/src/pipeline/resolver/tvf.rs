// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! TVF (Table-Valued Function) schema resolution
//!
//! TECHNICAL DEBT: This module hardcodes column schemas for known TVFs.
//! The correct fix is making the resolver permissive about column references
//! from Unknown-schema tables, then deleting this file entirely.

/// Hardcoded TVF schemas for known functions.
///
/// TECHNICAL DEBT: This should be replaced by runtime introspection.
/// TVF columns should be discovered by the backend at execution time,
/// with the resolver allowing Unknown-schema column references through.
pub(super) fn get_tvf_schema(
    function: &str,
    alias: Option<&str>,
    identities: &crate::relation::Planning,
) -> Option<crate::relation::SemanticRelation> {
    let table_name = alias.unwrap_or(function);
    let function_spelling = identities.intern(function, false);
    let entity = identities.mint_entity(function_spelling);
    let hint = identities.intern(table_name, false);
    let columns: &[(&str, &str)] = match function {
        "json_each" => &[
            ("key", "TEXT"),
            ("value", "TEXT"),
            ("type", "TEXT"),
            ("atom", "TEXT"),
            ("id", "INTEGER"),
            ("parent", "INTEGER"),
            ("fullkey", "TEXT"),
            ("path", "TEXT"),
        ],
        "pragma_table_info" => &[
            ("cid", "INTEGER"),
            ("name", "TEXT"),
            ("type", "TEXT"),
            ("notnull", "INTEGER"),
            ("dflt_value", "TEXT"),
            ("pk", "INTEGER"),
        ],
        // The catalog does not describe this one; its heading is the
        // target's, and the caller takes the default-transpilation road.
        _other => return None,
    };
    let slots: Vec<_> = columns
        .iter()
        .enumerate()
        .map(
            |(position, (name, data_type))| crate::relation::form::SourceSlot {
                position: position as u32,
                named: Some(identities.intern(name, false)),
                declared_type: Some((*data_type).to_string()),
            },
        )
        .collect();
    // A DESCRIBED TABLE-VALUED FUNCTION IS A SOURCE. Its complete
    // interface is part of the same construction act as its occurrence.
    identities
        .authority()
        .derive(crate::relation::RelForm::Source(
            crate::relation::form::SourceSpec {
                origin: crate::relation::form::SourceOrigin::TableValued { entity },
                slots: &slots,
                answers_to: Some(hint),
            },
        ))
        .ok()
}
