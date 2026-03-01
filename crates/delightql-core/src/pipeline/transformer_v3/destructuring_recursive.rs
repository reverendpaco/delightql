// Recursive Tree Group Destructuring
//
// Implementation of INDUCTIVE PRINCIPLE from INDUCTIVE-PRINCIPLE-AND-DESTRUCTURING-CODE-SMELL.md
//
// Solves for depth N (recursive case), and depth 1 falls out as base case.
// Pattern is a tree → walk it recursively.
//
// Key insight: json_each(json_extract(j1.value, '$.key')) works in SQLite!
// No CTEs needed - just build expression strings recursively.

use super::helpers::alias_generator::next_alias;
use crate::error::Result;
use crate::pipeline::ast_addressed::{
    CurlyMember, DestructureMode, DomainExpression, FunctionExpression,
};
use crate::pipeline::asts::core::expressions::functions::PathSegment;
use crate::pipeline::sql_ast_v3::{
    DomainExpression as SqlDomainExpression, SelectItem, TableExpression,
};

/// Result of recursive pattern walking
pub struct DestructureResult {
    /// SELECT items (json_extract calls)
    pub select_items: Vec<SelectItem>,

    /// Table joins (json_each calls)
    pub joins: Vec<TableExpression>,
}

/// Recursively walk a destructuring pattern
///
/// Arguments:
/// - pattern: The curly pattern to destructure
/// - current_source: SQL expression for current JSON source
///   - Depth 0: "json_col"
///   - Depth 1: "j1.value"
///   - Depth 2: "json_extract(j1.value, '$.nested')" (for nested array access)
/// - mode: Scalar (json_extract) vs Aggregate (LEFT JOIN json_each)
///
/// Returns: SELECT items and JOIN tables to add to query
pub fn walk_pattern_recursive(
    pattern: &FunctionExpression,
    current_source: String,
    mode: DestructureMode,
) -> Result<DestructureResult> {
    match pattern {
        // RECURSIVE CASE: Metadata Tree Group (country:~> {first_name})
        // Generate json_each, extract key as column, recurse into nested pattern
        FunctionExpression::MetadataTreeGroup {
            key_column,
            constructor,
            keys_only,
            ..
        } => {
            // Check if this is placeholder-only mode: country:~> {_}
            // This means "explode arrays but don't extract fields" (preserve cardinality)
            let is_placeholder_only = if let FunctionExpression::Curly { members, .. } = constructor.as_ref() {
                members.len() == 1 && matches!(members[0], CurlyMember::Placeholder)
            } else {
                false
            };

            // FIRST json_each: iterate over object keys
            let metadata_alias = next_alias();
            let metadata_join = create_json_each_join(
                &current_source,
                &metadata_alias,
            );

            // Extract the KEY as a column (this is the metadata!)
            // SELECT j1.key AS country
            let key_column_item = SelectItem::expression_with_alias(
                SqlDomainExpression::RawSql(format!("{}.key", metadata_alias)),
                key_column.clone()
            );

            // Keys-only mode (country:~> _): Return distinct keys without array explosion
            // This is for bare placeholder `_` (not `{_}`)
            if *keys_only {
                // Just extract the key column, no array explosion or field extraction
                return Ok(DestructureResult {
                    select_items: vec![key_column_item],
                    joins: vec![metadata_join],
                });
            }

            // Placeholder-only mode: explode arrays to preserve cardinality, but extract only keys
            // We need to do array explosion but skip field extraction
            if is_placeholder_only {
                // In aggregate mode, the metadata .value is an array that needs explosion
                match mode {
                    DestructureMode::Aggregate => {
                        // SECOND json_each: explode the array to preserve cardinality
                        let array_alias = next_alias();
                        let array_source = format!("{}.value", metadata_alias);
                        let array_join = create_json_each_join(
                            &array_source,
                            &array_alias,
                        );
                        // Return key column + both joins (metadata + array explosion)
                        // This preserves cardinality while only extracting the key
                        return Ok(DestructureResult {
                            select_items: vec![key_column_item],
                            joins: vec![metadata_join, array_join],
                        });
                    }
                    DestructureMode::Scalar => {
                        // Scalar mode: no array explosion needed
                        return Ok(DestructureResult {
                            select_items: vec![key_column_item],
                            joins: vec![metadata_join],
                        });
                    }
                }
            }

            // For metadata TG with Curly constructor, we ALWAYS need array explosion
            // because metadata .value is always an array, regardless of parent mode
            let (nested_source, array_explosion_join) = match &**constructor {
                FunctionExpression::Curly { .. } => {
                    // SECOND json_each: explode the array
                    let array_alias = next_alias();
                    let array_source = format!("{}.value", metadata_alias);
                    let array_join = create_json_each_join(
                        &array_source,
                        &array_alias,
                    );
                    // Recurse with the array element value
                    (format!("{}.value", array_alias), Some(array_join))
                }
                _ => {
                    // For nested MetadataTreeGroup or other patterns, just pass .value
                    (format!("{}.value", metadata_alias), None)
                }
            };

            // Recurse into nested constructor
            // After array explosion, switch to Scalar mode so Curly pattern
            // generates json_extract instead of another json_each
            let nested_mode = if matches!(&**constructor, FunctionExpression::Curly { .. })
                && array_explosion_join.is_some() {
                DestructureMode::Scalar
            } else {
                mode.clone()
            };

            let nested_result = walk_pattern_recursive(
                constructor.as_ref(),
                nested_source,
                nested_mode,
            )?;

            // Combine results: key column + nested results
            let mut select_items = vec![key_column_item];
            select_items.extend(nested_result.select_items);

            let mut joins = vec![metadata_join];
            if let Some(array_join) = array_explosion_join {
                joins.push(array_join);
            }
            joins.extend(nested_result.joins);

            Ok(DestructureResult {
                select_items,
                joins,
            })
        }

        FunctionExpression::Curly { members, .. } => {
            let mut select_items = Vec::new();
            let mut joins = Vec::new();

            for member in members {
                if std::env::var("DQL_DEBUG").is_ok() {
                    eprintln!("DESTRUCTURE MEMBER: {:?}", member);
                }
                match member {
                    // BASE CASE: Shorthand {first_name}
                    // Generate: json_extract(current_source, '$.first_name') AS first_name
                    CurlyMember::Shorthand { column, .. } => {
                        let item = generate_json_extract_from_string(
                            &current_source,
                            column,
                            column,
                        );
                        select_items.push(item);
                    }

                    // KeyValue without ~>: Could be simple mapping OR nested object
                    CurlyMember::KeyValue {
                        key,
                        nested_reduction: false,
                        value,
                    } => {
                        if std::env::var("DQL_DEBUG").is_ok() {
                            eprintln!("SCALAR KEYVALUE: key={}", key);
                        }
                        match value.as_ref() {
                            // BASE CASE: Simple mapping {"json_key": column_name}
                            // Generate: json_extract(current_source, '$.json_key') AS column_name
                            DomainExpression::Lvar { name, .. } => {
                                if std::env::var("DQL_DEBUG").is_ok() {
                                    eprintln!("SCALAR TVAR: key={}, name={}", key, name);
                                }
                                let item = generate_json_extract_from_string(
                                    &current_source,
                                    key,
                                    name,
                                );
                                select_items.push(item);
                            }

                            // RECURSIVE CASE (Scalar): Nested object {"address": {city, zip}}
                            // Build path: current_source + ".key"
                            // Then recurse into nested pattern
                            DomainExpression::Function(FunctionExpression::Curly { .. }) => {
                                let nested_pattern = match value.as_ref() {
                                    DomainExpression::Function(f @ FunctionExpression::Curly { .. }) => f,
                                    _ => unreachable!(),
                                };

                                // For nested objects, append to the JSON path
                                // If current_source is "data", new source is "json_extract(data, '$.address')"
                                // If current_source is "json_extract(data, '$.x')", new source is "json_extract(json_extract(data, '$.x'), '$.address')"
                                let nested_source = format!("json_extract({}, '$.{}')", current_source, key);

                                // RECURSE into nested pattern
                                let nested_result = walk_pattern_recursive(
                                    nested_pattern,
                                    nested_source,
                                    mode.clone(),
                                )?;

                                // Accumulate results
                                select_items.extend(nested_result.select_items);
                                joins.extend(nested_result.joins);
                            }

                            _ => {
                                return Err(crate::error::DelightQLError::validation_error(
                                    "Key-value destructuring requires either simple column reference or nested object pattern",
                                    "destructuring"
                                ));
                            }
                        }
                    }

                    // RECURSIVE CASE: Nested explosion {"users": ~> {first_name, last_name}}
                    // OR Aggregate TVar: {"users": ~> sub_users}
                    // 1. If value is Curly: Generate json_each and recurse
                    // 2. If value is Lvar: Aggregate TVar - capture whole value (no explosion)
                    CurlyMember::KeyValue {
                        key,
                        nested_reduction: true,
                        value,
                    } => {
                        // Check if this is Aggregate TVar (Lvar) or nested explosion (Curly)
                        match value.as_ref() {
                            // AGGREGATE TVAR: {"users": ~> sub_users}
                            // Extract from key, create column with value name
                            // json_extract(current_source, '$.users') AS sub_users
                            DomainExpression::Lvar { name, .. } => {
                                if std::env::var("DQL_DEBUG").is_ok() {
                                    eprintln!("AGGREGATE TVAR DEBUG: key={}, name={}", key, name);
                                }
                                let item = generate_json_extract_from_string(
                                    &current_source,
                                    key,      // JSON key to extract from
                                    name,     // Column name to create
                                );
                                select_items.push(item);
                            }

                            // NESTED EXPLOSION: {"users": ~> {first_name, last_name}}
                            // Generate json_each and recurse into pattern
                            DomainExpression::Function(FunctionExpression::Curly { .. }) => {
                                let nested_pattern = extract_nested_pattern(value)?;

                                // Generate unique alias for this json_each join
                                let join_alias = next_alias();

                                // Build json_each() TVF
                                // For top-level: json_each(json_col, '$.key')
                                // For nested: json_each(json_extract(j1.value, '$.key'), '$.inner_key')
                                // But we can simplify: json_each takes the source and path
                                //
                                // Actually, json_each(expr) takes just the JSON value
                                // To get nested: json_each(json_extract(j1.value, '$.users'))
                                //
                                // So we need to build: json_extract(current_source, '$.key')
                                let nested_json_source = format!("json_extract({}, '$.{}')", current_source, key);

                                // When nested_reduction is true, we're doing an aggregate explosion
                                // regardless of the parent mode.
                                let json_each_table = create_json_each_join(
                                    &nested_json_source,
                                    &join_alias,
                                );
                                joins.push(json_each_table);

                                // RECURSE: Walk nested pattern with "jN.value" as new source
                                let nested_source = format!("{}.value", join_alias);
                                let nested_result = walk_pattern_recursive(
                                    nested_pattern,
                                    nested_source,
                                    mode.clone(),
                                )?;

                                // Accumulate results from recursion
                                select_items.extend(nested_result.select_items);
                                joins.extend(nested_result.joins);
                            }

                            // NESTED ARRAY EXPLOSION: {"events": ~> [.0, .1]}
                            // Generate json_each and recurse into array pattern
                            DomainExpression::Function(FunctionExpression::Array { .. }) => {
                                // Extract the array pattern
                                let nested_pattern = match value.as_ref() {
                                    DomainExpression::Function(f @ FunctionExpression::Array { .. }) => f,
                                    _ => unreachable!("Already matched Array pattern"),
                                };

                                // Generate unique alias for this json_each join
                                let join_alias = next_alias();

                                // Extract the JSON array at this key
                                let nested_json_source = format!("json_extract({}, '$.{}')", current_source, key);

                                // Create json_each to explode the array
                                let json_each_table = create_json_each_join(
                                    &nested_json_source,
                                    &join_alias,
                                );
                                joins.push(json_each_table);

                                // RECURSE: Walk array pattern with "jN.value" as new source
                                // After explosion, switch to Scalar mode so array pattern
                                // extracts from individual elements, not another explosion
                                let nested_source = format!("{}.value", join_alias);
                                let nested_result = walk_pattern_recursive(
                                    nested_pattern,
                                    nested_source,
                                    DestructureMode::Scalar,
                                )?;

                                // Accumulate results from recursion
                                select_items.extend(nested_result.select_items);
                                joins.extend(nested_result.joins);
                            }

                            // NESTED METADATA TG: {"countries": ~> country:~> {first_name}}
                            // Pattern 6: Nested explosion with MetadataTreeGroup inside
                            //
                            // IMPORTANT: The json_each we create here ALREADY iterates the metadata
                            // object and extracts keys/values. So we should NOT recurse into the
                            // full MetadataTreeGroup (which would create ANOTHER json_each for keys).
                            // Instead, extract the metadata TG components and process them directly.
                            DomainExpression::Function(FunctionExpression::MetadataTreeGroup {
                                key_column,
                                constructor,
                                keys_only: _keys_only,
                                ..
                            }) => {
                                // Generate unique alias for this json_each join
                                let join_alias = next_alias();

                                // Extract the JSON value at this key
                                let nested_json_source = format!("json_extract({}, '$.{}')", current_source, key);

                                // This json_each iterates the metadata object (keys/values)
                                let json_each_table = create_json_each_join(
                                    &nested_json_source,
                                    &join_alias,
                                );
                                joins.push(json_each_table);

                                // Extract the KEY as a column (this is the metadata!)
                                // SELECT jN.key AS key_column
                                let key_column_item = SelectItem::expression_with_alias(
                                    SqlDomainExpression::RawSql(format!("{}.key", join_alias)),
                                    key_column.clone()
                                );
                                select_items.push(key_column_item);

                                // Now process the constructor (Curly or Array pattern) with the array value
                                // The .value from json_each is the array, which needs explosion
                                let array_alias = next_alias();
                                let array_source = format!("{}.value", join_alias);
                                let array_join = create_json_each_join(
                                    &array_source,
                                    &array_alias,
                                );
                                joins.push(array_join);

                                // Now recurse into the constructor (Curly or Array) with Scalar mode
                                // because we've already done the array explosion
                                let nested_source = format!("{}.value", array_alias);
                                let nested_result = walk_pattern_recursive(
                                    constructor.as_ref(),
                                    nested_source,
                                    DestructureMode::Scalar,
                                )?;

                                // Accumulate results from recursion
                                select_items.extend(nested_result.select_items);
                                joins.extend(nested_result.joins);
                            }

                            _ => {
                                return Err(crate::error::DelightQLError::validation_error(
                                    "Nested reduction (~>) requires either Curly pattern, Array pattern, MetadataTreeGroup, or identifier (TVar capture).\n\
                                     Examples:\n\
                                     - {\"users\": ~> {first_name}} - explode and extract object fields\n\
                                     - {\"events\": ~> [.0, .1]} - explode and extract array positions\n\
                                     - {\"countries\": ~> country:~> {name}} - explode with metadata TG\n\
                                     - {\"users\": ~> user_array} - capture whole array",
                                    "destructuring"
                                ));
                            }
                        }
                    }

                    // Placeholder: {_} - wildcard for destructuring (explode but don't extract fields)
                    CurlyMember::Placeholder => {
                        // {_} means "I know there's structure here but I don't want to extract any fields"
                        // Just validate the structure exists (by iterating) but extract nothing
                        // This is only useful for cardinality in aggregate destructuring
                        // No SELECT items generated, but the json_each still happens for row explosion
                    }

                    // PATH FIRST-CLASS: Epoch 6 - PathLiteral handling
                    CurlyMember::PathLiteral { path, alias } => {
                        // PathLiteral in destructuring: {.scripts.dev as dev_script}
                        // Extract segments from JsonPathLiteral and generate json_extract

                        // The path should be Projection(JsonPathLiteral) after resolution
                        match path.as_ref() {
                            DomainExpression::Projection(crate::pipeline::ast_addressed::ProjectionExpr::JsonPathLiteral { segments, .. }) => {
                                // Build the JSON path string (e.g., "scripts.dev")
                                let json_path = build_json_path_for_destructuring(segments)?;

                                // Determine column alias: use explicit alias or generate from path
                                let column_name: String = alias
                                    .as_ref()
                                    .map(|a| a.to_string())
                                    .unwrap_or_else(|| generate_alias_from_path(segments));

                                // Generate: json_extract(current_source, '$.scripts.dev') AS column_name
                                let item = generate_json_extract_from_string(
                                    &current_source,
                                    &json_path,
                                    &column_name,
                                );
                                select_items.push(item);
                            }
                            _ => {
                                return Err(crate::error::DelightQLError::transformation_error(
                                    "PathLiteral in destructuring must contain JsonPathLiteral expression",
                                    "destructuring",
                                ));
                            }
                        }
                    }

                    // Ergonomic inductors should be expanded by resolver
                    CurlyMember::Glob => {
                        return Err(crate::error::DelightQLError::ParseError {
                            message: "Glob inductor should have been expanded by resolver".to_string(),
                            source: None,
                            subcategory: None,
                        });
                    }
                    CurlyMember::Pattern { .. } => {
                        return Err(crate::error::DelightQLError::ParseError {
                            message: "Pattern inductor should have been expanded by resolver".to_string(),
                            source: None,
                            subcategory: None,
                        });
                    }
                    CurlyMember::OrdinalRange { .. } => {
                        return Err(crate::error::DelightQLError::ParseError {
                            message: "Ordinal range inductor should have been expanded by resolver".to_string(),
                            source: None,
                            subcategory: None,
                        });
                    }

                    // Comparison is for construction, not destructuring
                    CurlyMember::Comparison { .. } => {
                        return Err(crate::error::DelightQLError::validation_error(
                            "Comparison predicates not allowed in destructuring patterns",
                            "destructuring"
                        ));
                    }
                }
            }

            Ok(DestructureResult {
                select_items,
                joins,
            })
        }

        // ARRAY DESTRUCTURING: Epoch 5 - Handle [.0 as x, .1 as y] patterns
        FunctionExpression::Array { members, .. } => {
            use crate::pipeline::ast_addressed::ArrayMember;

            let mut select_items = Vec::new();
            let joins = Vec::new(); // No joins needed for scalar array destructuring

            // For aggregate mode, we need to json_each first, then extract indices
            let actual_source = match mode {
                DestructureMode::Aggregate => {
                    // In aggregate, current_source is already from json_each
                    current_source
                }
                DestructureMode::Scalar => {
                    // In scalar mode, use the source directly
                    current_source
                }
            };

            for member in members {
                match member {
                    ArrayMember::Index { path, alias } => {
                        // Extract nested path from Projection(JsonPathLiteral)
                        // Supports both simple indices [.0, .1] and nested paths [.0.name, .1.tags.0]
                        match path.as_ref() {
                            DomainExpression::Projection(crate::pipeline::ast_addressed::ProjectionExpr::JsonPathLiteral { segments, .. }) => {
                                // Build the JSON path string (e.g., "[0]", "[0].name", "[1].tags[0]")
                                let json_path = build_json_path_for_destructuring(segments)?;

                                // Determine column alias: use explicit alias or generate from path
                                let column_name: String = alias
                                    .as_ref()
                                    .map(|a| a.to_string())
                                    .unwrap_or_else(|| generate_alias_from_path(segments));

                                // Generate: json_extract(actual_source, '$.path') AS column_name
                                let item = generate_json_extract_from_string(
                                    &actual_source,
                                    &json_path,
                                    &column_name,
                                );
                                select_items.push(item);
                            }
                            _ => {
                                return Err(crate::error::DelightQLError::transformation_error(
                                    "Array member path must be JsonPathLiteral",
                                    "destructuring",
                                ));
                            }
                        }
                    }
                }
            }

            Ok(DestructureResult {
                select_items,
                joins,
            })
        }

        _ => {
            Err(crate::error::DelightQLError::ParseError {
                message: "Destructuring pattern must be a Curly function {...}, Array pattern [...], or Metadata Tree Group (key:~> ...)".to_string(),
                source: None,
                subcategory: None,
            })
        }
    }
}

/// Generate json_extract() SELECT item from string source
fn generate_json_extract_from_string(
    json_source_str: &str,
    json_key: &str,
    column_alias: &str,
) -> SelectItem {
    // Build: json_extract(json_source_str, '$path') AS column_alias
    // For object paths: '$.foo.bar'
    // For array paths: '$[0]' or '$[0].name'
    // The json_key already contains the path without '$', but may start with '[' for arrays
    let json_path = if json_key.starts_with('[') {
        // Array path: json_key is "[0]" or "[0].name", prepend just '$'
        format!("${}", json_key)
    } else {
        // Object path: json_key is "foo" or "foo.bar", prepend '$.'
        format!("$.{}", json_key)
    };

    let extract_sql = format!("json_extract({}, '{}')", json_source_str, json_path);
    SelectItem::expression_with_alias(
        SqlDomainExpression::RawSql(extract_sql),
        column_alias.to_string(),
    )
}

/// Create json_each() table-valued function reference (used as join target)
fn create_json_each_join(json_source_str: &str, alias: &str) -> TableExpression {
    TableExpression::TVF {
        schema: None,
        function: "json_each".to_string(),
        arguments: vec![crate::pipeline::sql_ast_v3::TvfArgument::Identifier(
            json_source_str.to_string(),
        )],
        alias: Some(alias.to_string()),
    }
}

/// Extract nested pattern from value (must be Curly function)
fn extract_nested_pattern(value: &Box<DomainExpression>) -> Result<&FunctionExpression> {
    match value.as_ref() {
        DomainExpression::Function(f @ FunctionExpression::Curly { .. }) => Ok(f),
        _ => Err(crate::error::DelightQLError::validation_error(
            "Nested destructuring ~> requires curly pattern.\n\
             Example: {\"users\": ~> {first_name, last_name}}",
            "destructuring",
        )),
    }
}

/// Build a JSON path string from path segments for destructuring
/// Generates a simplified path without the leading '$' (handled by caller)
///
/// Examples:
///   [ObjectKey("foo")] -> "foo"
///   [ObjectKey("foo"), ObjectKey("bar")] -> "foo.bar"
///   [ObjectKey("foo"), ArrayIndex(1)] -> "foo[1]"
///   [ObjectKey("foo"), ArrayIndex(1), ObjectKey("bar")] -> "foo[1].bar"
///   [ArrayIndex(0), ObjectKey("name")] -> "[0].name"
fn build_json_path_for_destructuring(segments: &[PathSegment]) -> Result<String> {
    let mut result = String::new();
    let mut previous_was_array = false;

    for (i, segment) in segments.iter().enumerate() {
        match segment {
            PathSegment::ObjectKey(key) => {
                // Add dot separator before object keys (except at the very start)
                if i > 0 && !previous_was_array {
                    result.push('.');
                } else if previous_was_array {
                    // After array index, we still need dot for object key
                    result.push('.');
                }
                result.push_str(key);
                previous_was_array = false;
            }
            PathSegment::ArrayIndex(idx) => {
                // Array indices don't get a dot separator - they're already bracketed
                result.push_str(&format!("[{}]", idx));
                previous_was_array = true;
            }
        }
    }

    Ok(result)
}

/// Generate a column alias from path segments
/// Examples:
///   [ObjectKey("name")] -> "name"
///   [ObjectKey("scripts"), ObjectKey("dev")] -> "scripts_dev"
///   [ArrayIndex(0), ObjectKey("name")] -> "0_name"
fn generate_alias_from_path(segments: &[PathSegment]) -> String {
    segments
        .iter()
        .map(|seg| match seg {
            PathSegment::ObjectKey(key) => key.clone(),
            PathSegment::ArrayIndex(idx) => idx.to_string(),
        })
        .collect::<Vec<_>>()
        .join("_")
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO: Add unit tests
    // Test depth 1: {a, b}
    // Test depth 2: {a, "nested": ~> {b}}
    // Test depth 3: {a, "n1": ~> {b, "n2": ~> {c}}}
}
