//! Unary operators and pipe operations

use super::metadata::NamespacePath;
use super::{
    Addressed, BooleanExpression, ContainmentSemantic, DomainExpression, DomainSpec,
    FunctionExpression, ModuloSpec, OrderingSpec, Refined, RenameSpec, RepositionSpec, Resolved,
    Unresolved,
};
use crate::{lispy::ToLispy, PhaseConvert, ToLispy};
use serde::{Deserialize, Serialize};

/// DML operation kind
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub enum DmlKind {
    #[lispy("dml_kind:update")]
    Update,
    #[lispy("dml_kind:delete")]
    Delete,
    #[lispy("dml_kind:insert")]
    Insert,
    #[lispy("dml_kind:keep")]
    Keep,
}

/// A single &-separated parameter group in an HO call.
///
/// Contains one or more ;-separated rows of comma-separated values.
/// For simple calls like `ho_view(users)`, this is one group with one row of one value.
/// For multi-row calls like `ho_view(1, "a"; 2, "b")`, this is one group with two rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
#[lispy("ho_call_group")]
pub struct HoCallGroup {
    /// ;-separated rows, each being comma-separated values (as text)
    pub rows: Vec<Vec<String>>,
}

impl HoCallGroup {
    /// Create a group with a single row of values.
    pub fn single_row(values: Vec<String>) -> Self {
        Self { rows: vec![values] }
    }

    /// Flatten to a single flat list of values (for legacy single-row, single-value groups).
    pub fn flat_values(&self) -> Vec<&str> {
        self.rows
            .iter()
            .flat_map(|row| row.iter().map(|s| s.as_str()))
            .collect()
    }

    /// Returns true if this is a single value (one row, one column).
    pub fn is_single_value(&self) -> bool {
        self.rows.len() == 1 && self.rows[0].len() == 1
    }

    /// Get the single value if this is a single-value group.
    pub fn as_single_value(&self) -> Option<&str> {
        if self.is_single_value() {
            Some(&self.rows[0][0])
        } else {
            None
        }
    }
}

/// Window frame specification for window functions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
#[lispy("window_frame")]
pub struct WindowFrame<Phase = Unresolved> {
    pub mode: FrameMode,
    pub start: FrameBound<Phase>,
    pub end: FrameBound<Phase>,
}

/// Frame mode for window functions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub enum FrameMode {
    #[lispy("frame_mode:groups")]
    Groups,
    #[lispy("frame_mode:rows")]
    Rows,
    #[lispy("frame_mode:range")]
    Range,
}

/// Frame bound for window functions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub enum FrameBound<Phase = Unresolved> {
    #[lispy("frame_bound:unbounded")]
    Unbounded,
    #[lispy("frame_bound:current_row")]
    CurrentRow,
    #[lispy("frame_bound:preceding")]
    Preceding(Box<DomainExpression<Phase>>),
    #[lispy("frame_bound:following")]
    Following(Box<DomainExpression<Phase>>),
}

/// Column selector for map operations - supports various selection patterns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub enum ColumnSelector<Phase = Unresolved> {
    /// Explicit columns: (col1, col2)
    #[lispy("column_selector:explicit")]
    Explicit(Vec<DomainExpression<Phase>>),
    /// Regex pattern: (/pattern/) - only in Unresolved phase
    #[lispy("column_selector:regex")]
    #[phase_convert(unreachable)]
    Regex(String),
    /// All columns: (*)
    #[lispy("column_selector:all")]
    All,
    /// Positional range: (|2:5|)
    #[lispy("column_selector:positional")]
    Positional { start: usize, end: usize },
    /// Multiple regex patterns: (/pattern1/, /pattern2/) - only in Unresolved phase
    #[lispy("column_selector:multi_regex")]
    #[phase_convert(unreachable)]
    MultipleRegex(Vec<String>),
    /// Resolved columns: final list of column names after resolution
    #[lispy("column_selector:resolved")]
    Resolved {
        columns: Vec<String>,
        original_selector: Box<ColumnSelector<Unresolved>>,
    },
}

/// Column alias for embed map cover operations
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub enum ColumnAlias {
    /// Literal alias: "foo"
    #[lispy("column_alias:literal")]
    Literal(String),
    /// Template with @ placeholder: "{@}_suffix"
    #[lispy("column_alias:template")]
    Template(ColumnNameTemplate),
}

/// Column name template containing @ placeholders
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub struct ColumnNameTemplate {
    /// Template string containing {@} placeholders
    pub template: String,
}

// Re-cored from refined.rs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub enum JoinType {
    /// Regular inner join (comma without markers)
    Inner,
    /// Left outer join (? on right table)
    LeftOuter,
    /// Right outer join (? on left table)
    RightOuter,
    /// Full outer join (? on both tables)
    FullOuter,
}

/// Operations applied through pipes
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub enum UnaryRelationalOperator<Phase = Unresolved> {
    /// General projection/selection: [...] or (...)
    #[lispy("unary_relational_operator:general")]
    General {
        containment_semantic: ContainmentSemantic,
        expressions: Vec<DomainExpression<Phase>>,
    },
    /// Modulo (distinct/group): %(...)
    #[lispy("unary_relational_operator:modulo")]
    Modulo {
        containment_semantic: ContainmentSemantic,
        spec: ModuloSpec<Phase>,
    },
    /// Tuple ordering: #(...) or #[...]
    #[lispy("unary_relational_operator:tuple_ordering")]
    TupleOrdering {
        containment_semantic: ContainmentSemantic,
        specs: Vec<OrderingSpec<Phase>>,
    },
    /// Map cover: $(f:(...))(...) or $(f:(...))[...]
    #[lispy("unary_relational_operator:map_cover")]
    MapCover {
        function: FunctionExpression<Phase>,
        columns: Vec<DomainExpression<Phase>>,
        containment_semantic: ContainmentSemantic,
        conditioned_on: Option<Box<BooleanExpression<Phase>>>,
    },
    /// Project out: -(...)
    #[lispy("unary_relational_operator:project_out")]
    ProjectOut {
        containment_semantic: ContainmentSemantic,
        expressions: Vec<DomainExpression<Phase>>,
    },
    /// Rename cover: *(...)
    #[lispy("unary_relational_operator:rename_cover")]
    RenameCover { specs: Vec<RenameSpec<Phase>> },
    /// Transform: $$(...) - many-to-many column transformations
    #[lispy("unary_relational_operator:transform")]
    Transform {
        transformations: Vec<(DomainExpression<Phase>, String, Option<String>)>, // (expression, alias, qualifier)
        conditioned_on: Option<Box<BooleanExpression<Phase>>>,
    },
    /// Aggregate pipe: |~>
    #[lispy("unary_relational_operator:aggregate_pipe")]
    AggregatePipe {
        aggregations: Vec<DomainExpression<Phase>>,
    },
    /// Reposition: |column as position| - move columns to specific positions
    #[lispy("unary_relational_operator:reposition")]
    Reposition { moves: Vec<RepositionSpec<Phase>> },
    /// Combined embed + map cover: +$(f)(...) - transform and add columns
    #[lispy("unary_relational_operator:embed_map_cover")]
    EmbedMapCover {
        function: FunctionExpression<Phase>,
        selector: ColumnSelector<Phase>,
        alias_template: Option<ColumnAlias>,
        containment_semantic: ContainmentSemantic,
    },
    /// Piped higher-order view application: source |> ho_view(cols) or source |> ho_view(args)(cols)
    ///
    /// Unresolved-only: the pipe handler inlines this into the expanded HO view body
    /// before resolution. It never appears in Resolved or Refined phase. Rust cannot
    /// express phase-conditional enum variants, so downstream match sites pay the
    /// exhaustive-match tax with unreachable!() arms.
    #[lispy("unary_relational_operator:ho_view_application")]
    #[phase_convert(unreachable)]
    HoViewApplication {
        function: String,
        arguments: Vec<HoCallGroup>,
        domain_spec: DomainSpec<Phase>,
        namespace: Option<NamespacePath>,
    },
    /// Meta-ize: ^ or ^^ - reifies relation schema as queryable data
    ///
    /// Single `^` returns basic column metadata (name, ordinal).
    /// Double `^^` returns detailed schema (type, nullable, constraints).
    /// Compile-time only: resolved during schema synthesis, produces virtual relation.
    #[lispy("unary_relational_operator:meta_ize")]
    MetaIze {
        /// True for `^^` (detailed), false for `^` (basic)
        detailed: bool,
    },
    /// Companion access: + or $ — query companion constraint/default tables
    ///
    /// Unresolved-only: the resolver materializes companion data from bootstrap
    /// into an inline Anonymous relation. Never survives past resolution.
    #[lispy("unary_relational_operator:companion_access")]
    #[phase_convert(unreachable)]
    CompanionAccess {
        kind: crate::pipeline::asts::ddl::CompanionKind,
    },
    /// Qualify: * - marks all columns as qualified (table-prefixed)
    ///
    /// Qualified columns don't unify implicitly with same-named columns from other tables.
    /// This is the opposite of empty parens `()` which introduces unqualified names.
    #[lispy("unary_relational_operator:qualify")]
    Qualify,
    /// Using: .(cols) - USING semantics (leftward search, unify, dedupe)
    ///
    /// Replaces *{cols} syntax. Performs:
    /// 1. Leftward search: find rightmost column matching each name in accumulated result
    /// 2. Unification: create join condition
    /// 3. Deduplication: remove one copy of unified column (USING semantics, not ON)
    #[lispy("unary_relational_operator:using")]
    Using { columns: Vec<String> },
    /// DML terminal: update!(table)(*), delete!(table)(*), insert!(table)(*), keep!(table)(*)
    ///
    /// The final pipe operator in a DML pipeline. Converts the upstream query
    /// into a SQL DML statement (DELETE, UPDATE, INSERT INTO ... SELECT).
    #[lispy("unary_relational_operator:dml_terminal")]
    DmlTerminal {
        kind: DmlKind,
        target: String,
        target_namespace: Option<String>,
        domain_spec: DomainSpec<Phase>,
    },
    /// Interior drill-down: .column_name(*) or .column_name(col1, col2)
    ///
    /// Explodes an interior relation (tree group) column into rows.
    /// Context columns are carried forward (lateral-join semantics).
    /// The interior_schema is None in the unresolved phase, populated by the resolver.
    /// Groundings: pairs of (schema_column_name, literal_value) for positional
    /// literal grounding — generates WHERE json_extract(value, '$.col') = 'val'.
    #[lispy("unary_relational_operator:interior_drill_down")]
    InteriorDrillDown {
        column: String,
        glob: bool,
        columns: Vec<String>,
        interior_schema: Option<Vec<InteriorColumnDef>>,
        #[serde(default)]
        groundings: Vec<(String, String)>,
    },
    /// Narrowing destructure: .column_name{.field1, .field2}
    ///
    /// Iterates a JSON array column via json_each, extracts named fields
    /// from each element via json_extract. No context carry-forward --
    /// the output schema contains only the named fields.
    #[lispy("unary_relational_operator:narrowing_destructure")]
    NarrowingDestructure { column: String, fields: Vec<String> },
    /// Directive pipe terminal: source |> directive!(args)
    ///
    /// Phase 1.X only — consumed by effect executor before resolution.
    /// The source directive produces rows; this terminal executes per-row.
    #[lispy("unary_relational_operator:directive_terminal")]
    #[phase_convert(unreachable)]
    DirectiveTerminal {
        /// Directive name (includes `!` suffix, e.g., "enlist!")
        name: String,
        /// Arguments from the call site. A glob (*) means "bind all upstream columns."
        arguments: Vec<DomainExpression<Phase>>,
    },
}

/// Definition of a column within an interior relation (tree group).
/// Used by InteriorDrillDown to know the schema of the interior relation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
#[lispy("interior_column_def")]
pub struct InteriorColumnDef {
    pub name: String,
    pub child_interior: Option<Vec<InteriorColumnDef>>,
}
