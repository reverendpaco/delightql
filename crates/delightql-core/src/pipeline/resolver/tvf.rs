// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! TVF (Table-Valued Function) schema resolution
//!
//! TECHNICAL DEBT: This module hardcodes column schemas for known TVFs.
//! The correct fix is making the resolver permissive about column references
//! from Unknown-schema tables, then deleting this file entirely.

/// Create column metadata for a TVF column
fn tvf_column(
    identities: &crate::names::Registry,
    scope: crate::names::ScopeId,
    entity: crate::names::EntityId,
    name: &str,
    data_type: &str,
    position: usize,
) -> crate::names::ColId {
    let published = identities.intern(name, false);
    identities.mint_column(
        scope,
        crate::names::ColumnOrigin::CatalogColumn {
            entity,
            position: position as u32,
        },
        Some(published),
        crate::names::Addressing::Published,
        crate::names::ValueFacts {
            declared_type: Some(data_type.to_string()),
            ..Default::default()
        },
    )
}

/// Hardcoded TVF schemas for known functions.
///
/// TECHNICAL DEBT: This should be replaced by runtime introspection.
/// TVF columns should be discovered by the backend at execution time,
/// with the resolver allowing Unknown-schema column references through.
pub(super) fn get_tvf_schema(
    function: &str,
    alias: Option<&str>,
    identities: &crate::names::Registry,
) -> Option<crate::names::ScopeId> {
    let table_name = alias.unwrap_or(function);
    let function_spelling = identities.intern(function, false);
    let entity = identities.mint_entity(function_spelling);
    let hint = identities.intern(table_name, false);
    let scope = identities.mint_scope(
        crate::names::ScopeOrigin::Resolution { of: entity },
        crate::names::Hint::User(hint),
        None,
    );

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
    for (position, (name, data_type)) in columns.iter().enumerate() {
        tvf_column(identities, scope, entity, name, data_type, position);
    }
    Some(scope)
}
