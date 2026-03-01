use crate::pipeline::asts::addressed as ast;
use crate::pipeline::danger_gates::DangerGateMap;
use crate::pipeline::generator_v3::SqlDialect;
use crate::pipeline::option_map::OptionMap;
use crate::pipeline::sql_ast_v3::{Cte, QueryExpression};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

/// Transform context for tracking correlation information during SQL transformation
/// This replaces the previous thread-local state with explicit context passing
#[derive(Clone)]
pub struct TransformContext {
    /// The current correlation alias for EXISTS subqueries and CPR references
    /// When transforming _.column references, this alias is used
    pub(crate) correlation_alias: Option<String>,

    /// Explicit alias remappings: original alias -> current alias
    /// When an inner table is wrapped in a subquery, its original alias maps to the wrapper alias.
    pub(crate) alias_remappings: Arc<HashMap<String, String>>,

    /// Whether to force CTEs to be emitted as WITH clauses (true) or inline as subqueries (false)
    pub(crate) force_ctes: bool,

    /// Map of CTE names to their query definitions (used when force_ctes = false)
    pub(crate) cte_definitions: Arc<HashMap<String, QueryExpression>>,

    /// Precompiled CFE definitions (for parameter substitution during transformation)
    pub(crate) cfe_definitions: Arc<Vec<ast::PrecompiledCfeDefinition>>,

    /// Generated CTEs that need to be included in the final WITH clause
    /// EPOCH 7: Used for premelt CTEs in melt/unpivot patterns
    pub(crate) generated_ctes: RefCell<Vec<Cte>>,

    /// Whether we're currently transforming aggregate expressions (reducing_on in modulo operator)
    /// Tree groups use this to decide between json_object vs json_group_array
    pub(crate) in_aggregate: bool,

    /// Qualifier policy snapshot from `SourceBinding`.
    /// When `Some`, the expression transformer uses this scope to decide
    /// whether to drop or preserve column qualifiers. `None` means no
    /// special policy (default: use AST qualifiers as-is).
    pub(in crate::pipeline::transformer_v3) qualifier_scope: Option<super::QualifierScope>,

    /// Target SQL dialect for dialect-specific transformations
    pub(crate) dialect: SqlDialect,

    /// Bin cartridge registry for entity lookup (sigma predicates, functions, etc.)
    /// Wrapped in Arc so context remains Clone
    pub(crate) bin_registry: Option<Arc<crate::bin_cartridge::registry::BinCartridgeRegistry>>,

    /// Danger gate states for this query (controls safety boundaries)
    /// Wrapped in Arc so context remains Clone without copying the map
    pub(crate) danger_gates: Arc<DangerGateMap>,

    /// Option states for this query (strategy/preference selection)
    /// Wrapped in Arc so context remains Clone without copying the map
    pub(crate) option_map: Arc<OptionMap>,

    /// Interior drill-down column mappings: (qualifier, column_name) → SQL expression.
    /// Set by InteriorDrillDown operator; read by expression transformer and
    /// schema-driven operators (project-out, rename) to emit correct SQL for
    /// interior columns that aren't backed by a real SQL table.
    /// Uses RefCell because the transformer passes &TransformContext immutably
    /// but the drill-down handler needs to populate the map.
    pub(crate) drill_column_mappings: RefCell<HashMap<String, String>>,
}

impl TransformContext {
    /// Create a new empty transform context with CTEs forced by default
    pub fn new(dialect: SqlDialect) -> Self {
        TransformContext {
            correlation_alias: None,
            alias_remappings: Arc::new(HashMap::new()),
            force_ctes: true, // Default: emit WITH clauses
            cte_definitions: Arc::new(HashMap::new()),
            cfe_definitions: Arc::new(Vec::new()),
            generated_ctes: RefCell::new(Vec::new()),
            in_aggregate: false,
            qualifier_scope: None,
            dialect,
            bin_registry: None,
            danger_gates: Arc::new(DangerGateMap::with_defaults()),
            option_map: Arc::new(OptionMap::with_defaults()),
            drill_column_mappings: RefCell::new(HashMap::new()),
        }
    }

    /// Create a context with force_ctes option
    pub fn with_force_ctes(force_ctes: bool, dialect: SqlDialect) -> Self {
        TransformContext {
            correlation_alias: None,
            alias_remappings: Arc::new(HashMap::new()),
            force_ctes,
            cte_definitions: Arc::new(HashMap::new()),
            cfe_definitions: Arc::new(Vec::new()),
            generated_ctes: RefCell::new(Vec::new()),
            in_aggregate: false,
            qualifier_scope: None,
            dialect,
            bin_registry: None,
            danger_gates: Arc::new(DangerGateMap::with_defaults()),
            option_map: Arc::new(OptionMap::with_defaults()),
            drill_column_mappings: RefCell::new(HashMap::new()),
        }
    }

    /// Set the aggregate mode, returning a new context
    pub fn set_aggregate(&self, in_aggregate: bool) -> Self {
        TransformContext {
            correlation_alias: self.correlation_alias.clone(),
            alias_remappings: self.alias_remappings.clone(),
            force_ctes: self.force_ctes,
            cte_definitions: self.cte_definitions.clone(),
            cfe_definitions: self.cfe_definitions.clone(),
            generated_ctes: self.generated_ctes.clone(),
            in_aggregate,
            qualifier_scope: self.qualifier_scope.clone(),
            dialect: self.dialect,
            bin_registry: self.bin_registry.clone(),
            danger_gates: self.danger_gates.clone(),
            option_map: self.option_map.clone(),
            drill_column_mappings: self.drill_column_mappings.clone(),
        }
    }

    /// Create a new context with additional alias remappings merged in
    pub fn with_additional_remappings(&self, new_remaps: &HashMap<String, String>) -> Self {
        let mut merged = (*self.alias_remappings).clone();
        merged.extend(new_remaps.iter().map(|(k, v)| (k.clone(), v.clone())));
        TransformContext {
            correlation_alias: self.correlation_alias.clone(),
            alias_remappings: Arc::new(merged),
            force_ctes: self.force_ctes,
            cte_definitions: self.cte_definitions.clone(),
            cfe_definitions: self.cfe_definitions.clone(),
            generated_ctes: self.generated_ctes.clone(),
            in_aggregate: self.in_aggregate,
            qualifier_scope: self.qualifier_scope.clone(),
            dialect: self.dialect,
            bin_registry: self.bin_registry.clone(),
            danger_gates: self.danger_gates.clone(),
            option_map: self.option_map.clone(),
            drill_column_mappings: self.drill_column_mappings.clone(),
        }
    }

    /// Return a new context carrying the given qualifier scope.
    /// The expression transformer will use this scope to decide
    /// whether to drop or preserve column qualifiers.
    pub(in crate::pipeline::transformer_v3) fn with_qualifier_scope(
        &self,
        scope: super::QualifierScope,
    ) -> Self {
        TransformContext {
            correlation_alias: self.correlation_alias.clone(),
            alias_remappings: self.alias_remappings.clone(),
            force_ctes: self.force_ctes,
            cte_definitions: self.cte_definitions.clone(),
            cfe_definitions: self.cfe_definitions.clone(),
            generated_ctes: self.generated_ctes.clone(),
            in_aggregate: self.in_aggregate,
            qualifier_scope: Some(scope),
            dialect: self.dialect,
            bin_registry: self.bin_registry.clone(),
            danger_gates: self.danger_gates.clone(),
            option_map: self.option_map.clone(),
            drill_column_mappings: self.drill_column_mappings.clone(),
        }
    }

    /// Add CTE definitions to the context (used when inlining CTEs as subqueries)
    pub fn with_cte_definitions(mut self, definitions: HashMap<String, QueryExpression>) -> Self {
        self.cte_definitions = Arc::new(definitions);
        self
    }

    /// Add CFE definitions to the context (for parameter substitution during transformation)
    pub fn with_cfe_definitions(mut self, definitions: Vec<ast::PrecompiledCfeDefinition>) -> Self {
        self.cfe_definitions = Arc::new(definitions);
        self
    }

    /// Add bin cartridge registry to the context (for entity lookup during transformation)
    pub fn with_bin_registry(
        mut self,
        registry: Arc<crate::bin_cartridge::registry::BinCartridgeRegistry>,
    ) -> Self {
        self.bin_registry = Some(registry);
        self
    }
}
