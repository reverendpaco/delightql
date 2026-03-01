//! Function expressions and related types
//! FunctionExpression, CurlyMember, CaseArm, StringTemplatePart

use super::super::{Addressed, LiteralValue, NamespacePath, Refined, Resolved, Unresolved};
use super::boolean::BooleanExpression;
use super::domain::DomainExpression;
use super::metadata_types::CteRequirements;
use crate::{lispy::ToLispy, PhaseConvert, ToLispy};
use delightql_types::SqlIdentifier;
use serde::{Deserialize, Serialize};

/// Segment in a JSON path
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToLispy)]
pub enum PathSegment {
    /// Object key access: .field or ."special-key"
    #[lispy("path_segment:object_key")]
    ObjectKey(String),

    /// Array index access: [n] (supports negative indices)
    #[lispy("path_segment:array_index")]
    ArrayIndex(i64),
}

/// Function expressions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub enum FunctionExpression<Phase = Unresolved> {
    /// Regular call: func(args) or namespace::path.func(args)
    #[lispy("domain_expression:function")]
    Regular {
        name: SqlIdentifier,
        namespace: Option<NamespacePath>,
        arguments: Vec<DomainExpression<Phase>>,
        alias: Option<SqlIdentifier>,
        conditioned_on: Option<Box<BooleanExpression<Phase>>>,
    },
    /// Curried call: func:(args) or namespace::path.func:(args)
    #[lispy("domain_expression:function:curried")]
    Curried {
        name: SqlIdentifier,
        namespace: Option<NamespacePath>,
        arguments: Vec<DomainExpression<Phase>>,
        conditioned_on: Option<Box<BooleanExpression<Phase>>>,
    },
    /// Higher-order CFE call: func:(curried_args)(regular_args)
    /// Example: apply_transform:(upper:())(first_name)
    #[lispy("domain_expression:function:higher_order")]
    HigherOrder {
        name: SqlIdentifier,
        curried_arguments: Vec<DomainExpression<Phase>>,
        regular_arguments: Vec<DomainExpression<Phase>>,
        alias: Option<SqlIdentifier>,
        conditioned_on: Option<Box<BooleanExpression<Phase>>>,
    },
    /// Bracket function: [expressions]
    #[lispy("domain_expression:function:bracket")]
    Bracket {
        arguments: Vec<DomainExpression<Phase>>,
        alias: Option<SqlIdentifier>,
    },
    /// Curly function (compound data constructor): {members}
    /// INTERIOR-RECORD: {name, "key": value, "nested": ~> {...}}
    #[lispy("domain_expression:function:curly")]
    Curly {
        members: Vec<CurlyMember<Phase>>,
        /// Columns that were promoted from non-nested members for CTE GROUP BY
        /// This preserves the tree group's internal grouping context
        inner_grouping_keys: Vec<DomainExpression<Phase>>,
        /// CTE requirements computed by resolver (Phase R2+)
        /// None during builder/early resolver, Some(...) after tree group analysis
        cte_requirements: Option<CteRequirements<Phase>>,
        alias: Option<SqlIdentifier>,
    },
    /// Array destructure pattern: [.0, .1, .2]
    /// ARRAY DESTRUCTURING: Epoch 2 - Extract positional elements from JSON arrays
    #[lispy("domain_expression:function:array")]
    Array {
        members: Vec<ArrayMember<Phase>>,
        alias: Option<SqlIdentifier>,
    },
    /// Metadata tree group: column:~> {constructor}
    /// Data values become JSON object keys (aggregate context only)
    #[lispy("domain_expression:function:metadata_tree_group")]
    MetadataTreeGroup {
        key_column: SqlIdentifier,
        key_qualifier: Option<SqlIdentifier>,
        key_schema: Option<SqlIdentifier>,
        constructor: Box<FunctionExpression<Phase>>, // Curly or Bracket
        /// True for bare placeholder (country:~> _) = keys only, no array explosion
        /// False for explicit Curly including {_} (country:~> {_}) = explode arrays
        keys_only: bool,
        /// CTE requirements computed by resolver (Phase R2+)
        /// None during builder/early resolver, Some(...) after tree group analysis
        cte_requirements: Option<CteRequirements<Phase>>,
        alias: Option<SqlIdentifier>,
    },
    /// Lambda function: :(expression) where expression contains @
    #[lispy("domain_expression:function:lambda")]
    Lambda {
        body: Box<DomainExpression<Phase>>,
        alias: Option<SqlIdentifier>,
    },
    /// Infix function: age * 2 == *(age, 2)
    #[lispy("domain_expression:function:infix")]
    Infix {
        operator: String,
        left: Box<DomainExpression<Phase>>,
        right: Box<DomainExpression<Phase>>,
        alias: Option<SqlIdentifier>,
    },
    /// String template: :"text {expr} text"
    #[lispy("domain_expression:function:string_template")]
    StringTemplate {
        parts: Vec<StringTemplatePart<Phase>>,
        alias: Option<SqlIdentifier>,
    },
    /// CASE expression: _:(cond -> result; ...)
    #[lispy("domain_expression:function:case")]
    CaseExpression {
        arms: Vec<CaseArm<Phase>>,
        alias: Option<SqlIdentifier>,
    },
    /// Window function: func:(args <~ partition, order, frame)
    #[lispy("domain_expression:function:window")]
    Window {
        name: SqlIdentifier,
        arguments: Vec<DomainExpression<Phase>>,
        partition_by: Vec<DomainExpression<Phase>>,
        order_by: Vec<super::super::OrderingSpec<Phase>>,
        frame: Option<super::super::operators::WindowFrame<Phase>>,
        alias: Option<SqlIdentifier>,
    },
    /// JSON path extraction: x:{path}
    /// Maps to: json_extract(x, '$<path>')
    /// PATH FIRST-CLASS: Epoch 2 - path is now a DomainExpression (can be literal or parameter)
    #[lispy("domain_expression:function:json_path")]
    JsonPath {
        /// Source expression (identifier before colon)
        source: Box<DomainExpression<Phase>>,
        /// Path expression (can be JsonPathLiteral or Parameter)
        path: Box<DomainExpression<Phase>>,
        alias: Option<SqlIdentifier>,
    },
}

/// Parts of a string template
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub enum StringTemplatePart<Phase = Unresolved> {
    /// Literal text segment
    #[lispy("template_part:text")]
    Text(String),
    /// Interpolated expression
    #[lispy("template_part:interpolation")]
    Interpolation(Box<DomainExpression<Phase>>),
}

/// Arms of a CASE expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub enum CaseArm<Phase = Unresolved> {
    /// Simple CASE: expr @ value -> result
    #[lispy("case_arm:simple")]
    Simple {
        test_expr: Box<DomainExpression<Phase>>,
        value: LiteralValue,
        result: Box<DomainExpression<Phase>>,
    },
    /// Curried Simple CASE: @ value -> result (for use with lambdas)
    #[lispy("case_arm:curried_simple")]
    CurriedSimple {
        value: LiteralValue,
        result: Box<DomainExpression<Phase>>,
    },
    /// Searched CASE: condition -> result
    #[lispy("case_arm:searched")]
    Searched {
        condition: Box<BooleanExpression<Phase>>,
        result: Box<DomainExpression<Phase>>,
    },
    /// Default case: _ -> result
    #[lispy("case_arm:default")]
    Default {
        result: Box<DomainExpression<Phase>>,
    },
}

/// Members of a curly function (compound data constructor)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, PhaseConvert)]
pub enum CurlyMember<Phase = Unresolved> {
    /// Shorthand: {name, email} → {"name": name, "email": email}
    /// Only works for lvars (column references), not arbitrary expressions
    Shorthand {
        column: SqlIdentifier,
        qualifier: Option<SqlIdentifier>,
        schema: Option<SqlIdentifier>,
    },

    /// Shorthand with predicate: {country="USA", age > 18}
    /// Grammar accepts any comparison, resolver validates one side is an lvar in scope
    Comparison {
        condition: Box<BooleanExpression<Phase>>,
    },

    /// Explicit key-value: {"key": value, "nested": ~> {...}}
    KeyValue {
        key: String,
        nested_reduction: bool, // true if "key": ~> value
        value: Box<DomainExpression<Phase>>,
    },

    /// Glob: {*} - expands to all available columns
    /// TG-ERGONOMIC-INDUCTOR: Resolved during resolver phase with de-duplication
    Glob,

    /// Pattern: {/name/} - expands to pattern-matched columns
    /// TG-ERGONOMIC-INDUCTOR: Resolved during resolver phase with de-duplication
    Pattern { pattern: String },

    /// Ordinal range: {|1:3|} - expands to columns in ordinal range
    /// TG-ERGONOMIC-INDUCTOR: Resolved during resolver phase with de-duplication
    OrdinalRange {
        start: Option<(u16, bool)>,
        end: Option<(u16, bool)>,
    },

    /// Placeholder: {_} - wildcard for destructuring (explode but don't extract fields)
    /// Only valid in destructuring context
    Placeholder,

    /// Path literal: {.scripts.dev} or {.name_info.last_name as ln}
    /// PATH FIRST-CLASS: Epoch 3 - direct JSON path extraction in destructuring
    PathLiteral {
        path: Box<DomainExpression<Phase>>, // Should be JsonPathLiteral
        alias: Option<SqlIdentifier>,
    },
}

/// Members of an array destructure pattern
/// ARRAY DESTRUCTURING: Epoch 2 - AST support for [.0, .1, .2] syntax
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, PhaseConvert)]
pub enum ArrayMember<Phase = Unresolved> {
    /// Positional index: [.0, .1, .2]
    /// Index must be a path literal (validated during builder phase)
    Index {
        path: Box<DomainExpression<Phase>>, // Should be JsonPathLiteral starting with integer
        alias: Option<SqlIdentifier>,
    },
}

// Manual ToLispy implementation for CurlyMember (can't derive due to lack of ToLispy for tuples)
impl<Phase> ToLispy for CurlyMember<Phase>
where
    BooleanExpression<Phase>: ToLispy,
    DomainExpression<Phase>: ToLispy,
{
    fn to_lispy(&self) -> String {
        match self {
            CurlyMember::Shorthand {
                column,
                qualifier,
                schema,
            } => {
                let qual_str = qualifier
                    .as_ref()
                    .map(|q| format!("{}", q))
                    .unwrap_or_default();
                let schema_str = schema
                    .as_ref()
                    .map(|s| format!("{}", s))
                    .unwrap_or_default();
                format!(
                    "(curly_member:shorthand {} {} {})",
                    column, qual_str, schema_str
                )
            }
            CurlyMember::Comparison { condition } => {
                format!("(curly_member:comparison {})", condition.to_lispy())
            }
            CurlyMember::KeyValue {
                key,
                nested_reduction,
                value,
            } => {
                format!(
                    "(curly_member:key_value {} {} {})",
                    key,
                    nested_reduction,
                    value.to_lispy()
                )
            }
            CurlyMember::Glob => "(curly_member:glob)".to_string(),
            CurlyMember::Pattern { pattern } => {
                format!("(curly_member:pattern {})", pattern)
            }
            CurlyMember::OrdinalRange { start, end } => {
                let format_pos = |(pos, rev): (u16, bool)| {
                    if rev {
                        format!("-{}", pos)
                    } else {
                        pos.to_string()
                    }
                };
                let start_str = start.map(format_pos).unwrap_or_default();
                let end_str = end.map(format_pos).unwrap_or_default();
                format!("(curly_member:ordinal_range {}:{})", start_str, end_str)
            }
            CurlyMember::Placeholder => "(curly_member:placeholder)".to_string(),
            CurlyMember::PathLiteral { path, alias } => {
                let alias_str = alias
                    .as_ref()
                    .map(|a| format!(" as {}", a))
                    .unwrap_or_default();
                format!(
                    "(curly_member:path_literal {}{})",
                    path.to_lispy(),
                    alias_str
                )
            }
        }
    }
}

// Manual ToLispy implementation for ArrayMember
impl<Phase> ToLispy for ArrayMember<Phase>
where
    DomainExpression<Phase>: ToLispy,
{
    fn to_lispy(&self) -> String {
        match self {
            ArrayMember::Index { path, alias } => {
                let alias_str = alias
                    .as_ref()
                    .map(|a| format!(" as {}", a))
                    .unwrap_or_default();
                format!("(array_member:index {}{})", path.to_lispy(), alias_str)
            }
        }
    }
}
