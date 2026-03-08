// Query Feature Detection
//
// This module defines the QueryFeature enum which tracks language features
// detected during the builder phase (CST → AST conversion).
//
// See QUERY-FEATURE-DETECTION.md for design rationale and usage.

use std::collections::{HashMap, HashSet};

/// Features detected in a DelightQL query during the build phase.
///
/// These represent observable syntax patterns in the query, not inferred semantics.
/// Detection happens at the builder level where we have first visibility into
/// the query structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryFeature {
    // === Relational Operations ===
    /// Join operations: users(*), orders(*)
    Joins,

    /// Outer joins: ?orders(*) - left/right/full
    OuterJoins,

    /// USING clause joins: orders(*{status})
    UsingClauses,

    // === CTEs ===
    /// Common Table Expressions: expr : name
    CTEs,

    // === CFEs ===
    /// Common Function Expressions: name:(params) : body
    CFEs,

    // === Aggregation ===
    /// Group by operations: |> %(...)
    GroupBy,

    /// Aggregate functions: sum:(), count:(), avg:(), etc
    AggregateFunctions,

    /// Filtered aggregates: count:(col | condition)
    FilteredAggregates,

    // === Pipe Operators ===
    /// Projection operator: |> (col1, col2)
    Projection,

    /// Map cover operator: |> $(fn)(cols)
    MapCover,

    /// Map embed operator: |> +$(fn)(cols) or |> +$(fn)(/pattern/)
    MapEmbed,

    /// Distinct operation: |> %(cols) without aggregates
    Distinct,

    /// Order by: |> ^(col1, col2)
    OrderBy,

    /// Limit clause: #<N
    Limit,

    /// Offset clause: #>N
    Offset,

    // === Functions & Expressions ===
    /// Table-valued functions: json_each('...')(*)
    TableValuedFunctions,

    /// Functional pipe (value-level): col /-> :(expr)
    FunctionalPipe,

    /// Lambda expressions with @ placeholder: :(@ + 10)
    LambdaExpressions,

    /// Function composition with nested @: upper:(trim:(@))
    FunctionComposition,

    /// Scalar subqueries: orders:(~> sum:(total))
    ScalarSubquery,

    // === Case Expressions ===
    /// Simple CASE: _:(val @ "a" -> 1; "b" -> 2)
    SimpleCaseExpr,

    /// Searched CASE: _:(cond1 -> val1; cond2 -> val2)
    SearchedCaseExpr,

    /// Curried CASE in pipe: value /-> _:(@ ...)
    CurriedCase,

    // === Column Selection ===
    /// Column ordinals: |1|, |-1|
    ColumnOrdinals,

    /// Column ranges: |1..3|, |1..|
    ColumnRanges,

    /// Pattern matching: /regex/
    PatternMatching,

    /// Glob selector: (*)
    Glob,

    // === String & Templates ===
    /// String templates: :"{first_name} {last_name}"
    StringTemplates,

    /// Positional templates: :"{@}_{#}" where @ = column, # = position
    PositionalTemplates,

    // === Set Operations ===
    /// UNION ALL CORRESPONDING: users_2022(*) ; users_2023(*)
    UnionCorresponding,

    // === Anonymous Tables ===
    /// Anonymous table literals: _(1, 2, 3)
    AnonymousTables,

    // === Pseudo-Predicates ===
    /// Pseudo-predicates: mount!(), enlist!(), delist!()
    PseudoPredicates,

    // === Advanced ===
    /// Qualified names: schema.table or table.column
    QualifiedNames,

    /// Aliases: as alias_name
    Aliases,

    /// Arithmetic expressions: col1 + col2 * 3
    ArithmeticExpressions,

    /// Boolean expressions: col1 = 5, col2 > 10
    BooleanExpressions,
}

/// HO parameter bindings threaded through the builder for AST-level substitution.
///
/// When a view body is parsed with HO bindings active, the builder substitutes
/// param names at construction time instead of using text-level regex replacement.
#[derive(Debug, Clone, Default)]
pub struct HoParamBindings {
    /// Glob: param_name → actual table name
    pub table_params: HashMap<String, String>,
    /// Argumentative: param_name → anonymous table RelationalExpression
    pub table_expr_params: HashMap<String, crate::pipeline::asts::unresolved::RelationalExpression>,
    /// Scalar: param_name → DomainExpression value
    pub scalar_params: HashMap<String, crate::pipeline::asts::unresolved::DomainExpression>,
    /// Pending arity checks for argumentative params that received table references.
    /// (param_name, table_name, expected_column_count, column_names)
    pub argumentative_table_refs: Vec<(String, String, usize, Vec<String>)>,
    /// Remap from argumentative lvar names to (table_name, actual_column_name).
    /// E.g., V(k, l) bound to refs(key, label) → {k → ("refs", "key"), l → ("refs", "label")}.
    /// Built after arity validation when actual column names are known.
    pub argumentative_column_remap: HashMap<String, (String, String)>,
}

impl HoParamBindings {
    pub fn is_empty(&self) -> bool {
        self.table_params.is_empty()
            && self.table_expr_params.is_empty()
            && self.scalar_params.is_empty()
    }
}

/// Context for tracking features and collecting assertions, emits, dangers, options, and DDL blocks during building
#[derive(Debug, Default)]
pub struct FeatureCollector {
    features: HashSet<QueryFeature>,
    assertions: Vec<crate::pipeline::asts::core::AssertionSpec>,
    emits: Vec<crate::pipeline::asts::core::EmitSpec>,
    dangers: Vec<crate::pipeline::asts::core::DangerSpec>,
    options: Vec<crate::pipeline::asts::core::OptionSpec>,
    ddl_blocks: Vec<crate::pipeline::asts::core::InlineDdlSpec>,
    pub ho_bindings: Option<HoParamBindings>,
}

impl FeatureCollector {
    pub fn new() -> Self {
        Self {
            features: HashSet::new(),
            assertions: Vec::new(),
            emits: Vec::new(),
            dangers: Vec::new(),
            options: Vec::new(),
            ddl_blocks: Vec::new(),
            ho_bindings: None,
        }
    }

    /// Create a child collector that inherits ho_bindings but is otherwise fresh.
    pub fn inheriting_ho_bindings(parent: &Self) -> Self {
        let mut fc = Self::new();
        fc.ho_bindings = parent.ho_bindings.clone();
        fc
    }

    /// Mark a feature as detected
    pub fn mark(&mut self, feature: QueryFeature) {
        self.features.insert(feature);
    }

    /// Get the collected features
    pub fn into_features(self) -> HashSet<QueryFeature> {
        self.features
    }

    /// Add a data assertion collected during continuation processing
    pub fn add_assertion(&mut self, spec: crate::pipeline::asts::core::AssertionSpec) {
        self.assertions.push(spec);
    }

    /// Take collected assertions (leaves the internal vec empty)
    pub fn take_assertions(&mut self) -> Vec<crate::pipeline::asts::core::AssertionSpec> {
        std::mem::take(&mut self.assertions)
    }

    /// Add an emit spec collected during continuation processing
    pub fn add_emit(&mut self, spec: crate::pipeline::asts::core::EmitSpec) {
        self.emits.push(spec);
    }

    /// Take collected emits (leaves the internal vec empty)
    pub fn take_emits(&mut self) -> Vec<crate::pipeline::asts::core::EmitSpec> {
        std::mem::take(&mut self.emits)
    }

    /// Add a danger spec collected during continuation processing
    pub fn add_danger(&mut self, spec: crate::pipeline::asts::core::DangerSpec) {
        self.dangers.push(spec);
    }

    /// Take collected dangers (leaves the internal vec empty)
    pub fn take_dangers(&mut self) -> Vec<crate::pipeline::asts::core::DangerSpec> {
        std::mem::take(&mut self.dangers)
    }

    /// Add an option spec collected during continuation processing
    pub fn add_option(&mut self, spec: crate::pipeline::asts::core::OptionSpec) {
        self.options.push(spec);
    }

    /// Take collected options (leaves the internal vec empty)
    pub fn take_options(&mut self) -> Vec<crate::pipeline::asts::core::OptionSpec> {
        std::mem::take(&mut self.options)
    }

    /// Add an inline DDL block collected during query parsing
    pub fn add_ddl_block(&mut self, spec: crate::pipeline::asts::core::InlineDdlSpec) {
        self.ddl_blocks.push(spec);
    }

    /// Take collected DDL blocks (leaves the internal vec empty)
    pub fn take_ddl_blocks(&mut self) -> Vec<crate::pipeline::asts::core::InlineDdlSpec> {
        std::mem::take(&mut self.ddl_blocks)
    }
}
