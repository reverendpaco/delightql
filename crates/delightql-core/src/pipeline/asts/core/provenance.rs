//! Column provenance / identity stack infrastructure (Epoch 1)
//!
//! Tracks how a column's identity evolves through the pipeline:
//! original table → user alias → pipe barrier → CTE registration → subquery alias → …

use crate::lispy::ToLispy;
use delightql_types::SqlIdentifier;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smallvec::{smallvec, SmallVec};
use std::sync::Arc;

use super::metadata::TableName;

// Serde support for Arc (serialize/deserialize by cloning the inner value)
mod serde_arc {
    use super::*;

    pub fn serialize<S, T>(value: &Arc<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        value.as_ref().serialize(serializer)
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<Arc<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        T::deserialize(deserializer).map(Arc::new)
    }
}

/// Represents the pipeline phase when an identity transformation occurred
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TransformationPhase {
    Unresolved,
    Resolved,
    Refined,
    Transformer,
}

/// Context describing WHY a column has this particular identity at this point
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IdentityContext {
    /// Original table column
    OriginalTable {
        table: TableName,
        was_qualified: bool,
    },

    /// User applied an alias via `as`
    UserAlias { previous_name: String },

    /// Column passed through a pipe operator
    PipeBarrier {
        previous_table: TableName,
        fresh_scope: usize,
    },

    /// Column registered as part of a CTE
    CteRegistration { cte_name: String },

    /// Query wrapped in subquery with alias (Transformer phase)
    /// This tracks when columns are re-scoped to a subquery alias
    SubqueryAlias {
        alias: String,
        previous_context: String, // What it was qualified as before (e.g., "active", "users")
    },

    /// MAP-COVER transformation applied
    MapCoverTransform { function: String },

    /// Generated name for expression/function
    Generated { reason: String, position: usize },

    /// Positional pattern matching (Danger Zone 1)
    PositionalPattern {
        table: String,
        position: usize,
        matched_column: String,
    },

    /// USING unification in joins (Danger Zone 1)
    UsingUnification {
        left_table: String,
        right_table: String,
        kept_side: UnificationSide,
    },

    /// Correlation variable in EXISTS (Danger Zone 2)
    CorrelationVariable {
        alias: String,
        outer_table: String,
        outer_cte: Option<String>,
    },

    /// Column inside EXISTS subquery (Danger Zone 2)
    ExistsSubquery {
        exists_identifier: String,
        correlation_depth: usize,
    },

    /// Recursive CTE self-reference (Danger Zone 6)
    RecursiveCteSelfReference {
        cte_name: String,
        head_index: usize,
        is_base_case: bool,
    },

    /// UNION CORRESPONDING merge (Danger Zone 6)
    UnionCorrespondingMerge { kept_operand_index: usize },
}

/// Which side was kept in USING unification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UnificationSide {
    Left,
    Right,
}

impl IdentityContext {
    /// Short description for debug/trace output
    pub fn short_desc(&self) -> &'static str {
        match self {
            IdentityContext::OriginalTable { .. } => "OriginalTable",
            IdentityContext::UserAlias { .. } => "UserAlias",
            IdentityContext::PipeBarrier { .. } => "PipeBarrier",
            IdentityContext::CteRegistration { .. } => "CteRegistration",
            IdentityContext::SubqueryAlias { .. } => "SubqueryAlias",
            IdentityContext::MapCoverTransform { .. } => "MapCoverTransform",
            IdentityContext::Generated { .. } => "Generated",
            IdentityContext::PositionalPattern { .. } => "PositionalPattern",
            IdentityContext::UsingUnification { .. } => "UsingUnification",
            IdentityContext::CorrelationVariable { .. } => "CorrelationVariable",
            IdentityContext::ExistsSubquery { .. } => "ExistsSubquery",
            IdentityContext::RecursiveCteSelfReference { .. } => "RecursiveCteSelfReference",
            IdentityContext::UnionCorrespondingMerge { .. } => "UnionCorrespondingMerge",
        }
    }
}

/// Single identity snapshot in the temporal stack
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnIdentity {
    /// The name at this point in time
    pub name: SqlIdentifier,

    /// The context that gave rise to this identity
    pub context: IdentityContext,

    /// When in the pipeline this identity was created
    pub phase: TransformationPhase,

    /// The SQL qualifier for this column at this point in the stack.
    ///
    /// INCOMPLETE: Only populated at `with_identity()` call sites (8 total: pipe barriers,
    /// CTE registration, subquery aliases). Columns that never pass through these sites
    /// carry the default from their constructor (typically `Fresh`). Because of this,
    /// `current_table_qualifier()` cannot yet replace the expression transformer's
    /// priority chain, which also consults `fq_table.name` and AST qualifiers.
    /// See `memory/provenance-table-qualifier.md` for the full diagnosis.
    pub table_qualifier: TableName,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnProvenance {
    /// Temporal identity stack (most recent first)
    /// Arc<SmallVec> optimization: O(1) clones even with deep stacks (50+ pipes)
    /// SmallVec optimization: typically 2-4 entries, no heap allocation
    #[serde(with = "serde_arc")]
    identity_stack: Arc<SmallVec<[ColumnIdentity; 4]>>,
}

impl ColumnProvenance {
    // ========================================================================
    // Constructors (build initial identity stack)
    // ========================================================================

    /// Create from table column (original table context)
    pub fn from_table_column(
        name: impl Into<SqlIdentifier>,
        table: TableName,
        was_qualified: bool,
    ) -> Self {
        let name = name.into();
        let stack = smallvec![ColumnIdentity {
            name: name.clone(),
            context: IdentityContext::OriginalTable {
                table: table.clone(),
                was_qualified,
            },
            phase: TransformationPhase::Resolved,
            table_qualifier: table,
        }];

        ColumnProvenance {
            identity_stack: Arc::new(stack),
        }
    }

    /// Create from column name (for backward compatibility, assumes unqualified)
    pub fn from_column(name: impl Into<SqlIdentifier>) -> Self {
        let name = name.into();
        // Create a minimal stack without table context - callers that need
        // table provenance should use from_table_column() instead
        let stack = smallvec![ColumnIdentity {
            name: name.clone(),
            context: IdentityContext::OriginalTable {
                table: TableName::Fresh, // Placeholder - should be updated by caller
                was_qualified: false,
            },
            phase: TransformationPhase::Resolved,
            table_qualifier: TableName::Fresh,
        }];

        ColumnProvenance {
            identity_stack: Arc::new(stack),
        }
    }

    /// Create from generated/unnamed expression
    pub fn from_generated(name: impl Into<SqlIdentifier>, reason: String, position: usize) -> Self {
        let name = name.into();
        let stack = smallvec![ColumnIdentity {
            name: name.clone(),
            context: IdentityContext::Generated { reason, position },
            phase: TransformationPhase::Resolved,
            table_qualifier: TableName::Fresh,
        }];

        ColumnProvenance {
            identity_stack: Arc::new(stack),
        }
    }

    // ========================================================================
    // Builders (push identity transformations)
    // ========================================================================

    /// Push a new identity onto the stack (immutable)
    pub fn with_identity(mut self, identity: ColumnIdentity) -> Self {
        // Use Arc::make_mut for copy-on-write: only clones if Arc has multiple owners
        let stack = Arc::make_mut(&mut self.identity_stack);
        stack.insert(0, identity);
        self
    }

    /// Apply user alias
    pub fn with_alias(mut self, alias: impl Into<SqlIdentifier>) -> Self {
        let alias = alias.into();
        // Push UserAlias identity, inheriting table_qualifier from top of stack
        let previous_name = self.name().unwrap_or("<unnamed>").to_string();
        let inherited_qualifier = self
            .identity_stack
            .first()
            .map(|id| id.table_qualifier.clone())
            .unwrap_or(TableName::Fresh);

        // Use Arc::make_mut for copy-on-write
        let stack = Arc::make_mut(&mut self.identity_stack);
        stack.insert(
            0,
            ColumnIdentity {
                name: alias.clone(),
                context: IdentityContext::UserAlias { previous_name },
                phase: TransformationPhase::Resolved,
                table_qualifier: inherited_qualifier,
            },
        );
        self
    }

    /// Promote the effective (top-of-stack) name to the bottom of the stack.
    ///
    /// Called at pipe boundaries after pushing PipeBarrier. After promotion,
    /// `original_name()` returns the same as `name()` — the column's identity
    /// is sealed. The alias (if any) has been consumed; downstream code sees
    /// a single unambiguous name regardless of which accessor it uses.
    pub fn promote_at_barrier(mut self) -> Self {
        let stack = Arc::make_mut(&mut self.identity_stack);
        if stack.len() > 1 {
            if let Some(effective) = stack.first().map(|e| e.name.clone()) {
                if let Some(bottom) = stack.last_mut() {
                    bottom.name = effective;
                }
            }
        }
        self
    }

    /// Update was_qualified flag in the identity stack
    ///
    /// Used by resolver during unification when the actual reference style is determined.
    /// This updates the OriginalTable context's was_qualified field to match how the
    /// user actually referenced the column in the query.
    ///
    /// This is NOT a transformation (doesn't push new identity), but rather a refinement
    /// of the existing OriginalTable context once we know how it was referenced.
    pub fn with_updated_qualification(mut self, was_qualified: bool) -> Self {
        // Use Arc::make_mut for copy-on-write: only clones if Arc has multiple owners
        let stack = Arc::make_mut(&mut self.identity_stack);

        // Find and update the OriginalTable context (should be at bottom/last of stack)
        // Walk from bottom up to find the first OriginalTable context
        for identity in stack.iter_mut().rev() {
            if let IdentityContext::OriginalTable {
                was_qualified: ref mut wq,
                ..
            } = &mut identity.context
            {
                *wq = was_qualified;
                break;
            }
        }
        self
    }

    // ========================================================================
    // Query methods (delegate to stack)
    // ========================================================================

    /// Get current name (top of stack)
    pub fn name(&self) -> Option<&str> {
        self.identity_stack.first().map(|i| i.name.as_str())
    }

    /// Get original name (bottom of stack)
    pub fn original_name(&self) -> Option<&str> {
        self.identity_stack.last().map(|i| i.name.as_str())
    }

    /// Check if column has an alias
    pub fn has_alias(&self) -> bool {
        self.identity_stack
            .iter()
            .any(|id| matches!(id.context, IdentityContext::UserAlias { .. }))
    }

    /// Get alias name if present
    pub fn alias_name(&self) -> Option<&str> {
        self.identity_stack.iter().find_map(|id| match &id.context {
            IdentityContext::UserAlias { .. } => Some(id.name.as_str()),
            _ => None,
        })
    }

    /// Get the name this column had before the most recent alias.
    /// For renamed columns, this is the source name in the input relation.
    /// Searches the identity stack for the most recent UserAlias.
    /// Returns None if no alias exists.
    pub fn source_name(&self) -> Option<&str> {
        self.identity_stack.iter().find_map(|id| match &id.context {
            IdentityContext::UserAlias { previous_name } => Some(previous_name.as_str()),
            _ => None,
        })
    }

    /// Check if column was qualified in source
    pub fn is_qualified(&self) -> Option<bool> {
        // Walk stack to find original table context
        self.identity_stack
            .iter()
            .rev()
            .find_map(|id| match &id.context {
                IdentityContext::OriginalTable { was_qualified, .. } => Some(*was_qualified),
                _ => None,
            })
    }

    /// Current SQL qualifier from top of identity stack.
    /// Currently only useful for diagnostics — see `table_qualifier` field doc.
    pub fn current_table_qualifier(&self) -> Option<&TableName> {
        self.identity_stack.first().map(|id| &id.table_qualifier)
    }

    // ========================================================================
    // New stack query methods (Epoch 1)
    // ========================================================================

    /// Find the most recent CTE context (walk stack backward from top)
    pub fn current_cte_context(&self) -> Option<&str> {
        // Search from NEWEST to OLDEST (reverse order) to get most recent context
        let result = self
            .identity_stack
            .iter()
            .rev()
            .find_map(|id| match &id.context {
                IdentityContext::CteRegistration { cte_name } => Some(cte_name.as_str()),
                IdentityContext::SubqueryAlias { alias, .. } => Some(alias.as_str()),
                _ => None,
            });

        log::debug!(
            "current_cte_context() for column '{}': {:?} (stack: {})",
            self.name().unwrap_or("<no-name>"),
            result,
            self.trace_lineage()
        );

        result
    }

    /// Get the CTE name that can be used to REFERENCE this column in SQL
    ///
    /// This is different from current_cte_context() which returns any context.
    /// This method ONLY returns CteRegistration - these are CTEs in the WITH clause
    /// that are referenceable by name in SQL.
    ///
    /// Returns the effective SQL qualifier for this column based on its provenance.
    ///
    /// Walks the identity stack (newest first) looking for the first naming context:
    /// - SubqueryAlias: the CTE was wrapped as `FROM cte AS alias` — use the alias
    /// - CteRegistration: the CTE is referenced directly without alias — use the CTE name
    ///
    /// The SubqueryAlias carries the SQL alias that appears in the FROM clause,
    /// which is exactly the qualifier that SELECT items must use.
    pub fn referenceable_cte_name(&self) -> Option<&str> {
        let mut result = None;
        for id in self.identity_stack.iter() {
            match &id.context {
                IdentityContext::SubqueryAlias { alias, .. } => {
                    // Only return this alias if there's a CteRegistration deeper
                    // in the stack — that means this is a CTE referenced with an
                    // alias (e.g. `active(*) as a`), not a pipe barrier subquery.
                    let has_cte_below = self
                        .identity_stack
                        .iter()
                        .any(|i| matches!(&i.context, IdentityContext::CteRegistration { .. }));
                    if has_cte_below {
                        result = Some(alias.as_str());
                    }
                    break;
                }
                IdentityContext::CteRegistration { cte_name } => {
                    result = Some(cte_name.as_str());
                    break;
                }
                _ => {}
            }
        }

        log::debug!(
            "referenceable_cte_name() for column '{}': {:?} (stack: {})",
            self.name().unwrap_or("<no-name>"),
            result,
            self.trace_lineage()
        );

        result
    }

    /// Get the last TableName context before it became Fresh
    pub fn last_named_table(&self) -> Option<&TableName> {
        self.identity_stack
            .iter()
            .rev()
            .find_map(|id| match &id.context {
                IdentityContext::OriginalTable { table, .. } => Some(table),
                IdentityContext::PipeBarrier { previous_table, .. } => Some(previous_table),
                _ => None,
            })
    }

    /// Full lineage trace for debugging
    pub fn trace_lineage(&self) -> String {
        if self.identity_stack.is_empty() {
            return "<empty stack>".to_string();
        }

        self.identity_stack
            .iter()
            .rev()
            .map(|id| format!("{} ({})", id.name, id.context.short_desc()))
            .collect::<Vec<_>>()
            .join(" → ")
    }

    /// Get the identity stack (for advanced use cases)
    pub fn identity_stack(&self) -> &[ColumnIdentity] {
        &self.identity_stack
    }
}

impl ToLispy for ColumnProvenance {
    fn to_lispy(&self) -> String {
        // Output using stack query methods
        let mut parts = vec![];

        if let Some(orig) = self.original_name() {
            parts.push(format!("original:{}", orig));
        }
        if let Some(alias) = self.alias_name() {
            parts.push(format!("alias:{}", alias));
        }

        format!("(column_spec {})", parts.join(" "))
    }
}
