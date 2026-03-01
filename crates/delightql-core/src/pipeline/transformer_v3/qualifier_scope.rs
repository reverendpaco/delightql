use crate::pipeline::sql_ast_v3::{ColumnQualifier, DomainExpression};

/// Capability token for ColumnQualifier construction.
///
/// Only `qualifier_scope.rs` can construct this (private field).
/// Transformer code uses `QualifierScope::structural()` which
/// constructs mints internally. External code cannot construct
/// qualifiers without a mint — the compiler enforces this.
pub struct QualifierMint(());

impl QualifierMint {
    /// Test-only escape hatch for code outside transformer_v3.
    #[cfg(test)]
    pub fn for_test() -> Self {
        QualifierMint(())
    }
}

/// Qualification policy for the current data source scope.
///
/// Clone: safe to snapshot into `TransformContext` as a read-only policy.
/// Move-semantics enforcement lives in `SourceBinding` (consumes `self`
/// on `descend_into_cte` / `unwrap_to_join`), not here.
#[derive(Clone)]
pub(in crate::pipeline::transformer_v3) enum QualifierScope {
    /// Columns are unambiguous — drop all qualifiers.
    /// Used for: simple Table, Subquery (before unwrap), CTE body construction.
    Unqualified,

    /// Source was unwrapped from subquery into a join — qualifiers needed.
    /// Columns preserve their AST qualifier; unqualified columns fall back
    /// to `base_alias`.
    JoinSource { base_alias: String },

    /// Source is a CTE from inner recursion — columns are unqualified.
    /// `cte_name` and `col_aliases` enable key rewriting for recursive descent.
    CteSource {
        cte_name: String,
        col_aliases: Vec<String>,
    },
}

impl QualifierScope {
    pub(in crate::pipeline::transformer_v3) fn should_drop_qualifiers(&self) -> bool {
        matches!(self, Self::Unqualified | Self::CteSource { .. })
    }

    pub(in crate::pipeline::transformer_v3) fn did_recurse(&self) -> bool {
        matches!(self, Self::CteSource { .. })
    }

    pub(in crate::pipeline::transformer_v3) fn qualify_column(
        &self,
        name: &str,
        ast_qualifier: Option<&delightql_types::SqlIdentifier>,
    ) -> DomainExpression {
        match self {
            Self::Unqualified | Self::CteSource { .. } => DomainExpression::Column {
                name: name.to_string(),
                qualifier: None,
            },
            Self::JoinSource { base_alias } => {
                let q = ast_qualifier
                    .map(|q| q.to_string())
                    .unwrap_or_else(|| base_alias.clone());
                DomainExpression::Column {
                    name: name.to_string(),
                    qualifier: Some(ColumnQualifier::table(q, &QualifierMint(()))),
                }
            }
        }
    }

    /// CTE identity for key rewriting. None if not a CTE source.
    pub(in crate::pipeline::transformer_v3) fn cte_identity(&self) -> Option<(&str, &[String])> {
        match self {
            Self::CteSource {
                cte_name,
                col_aliases,
            } => Some((cte_name, col_aliases)),
            _ => None,
        }
    }

    /// Transition into CTE source. Consumes self.
    pub(in crate::pipeline::transformer_v3) fn descend_into_cte(
        self,
        cte_name: String,
        col_aliases: Vec<String>,
    ) -> Self {
        Self::CteSource {
            cte_name,
            col_aliases,
        }
    }

    /// Transition from Unqualified (subquery) to JoinSource (unwrapped join).
    pub(in crate::pipeline::transformer_v3) fn unwrap_to_join(self, base_alias: String) -> Self {
        debug_assert!(matches!(self, Self::Unqualified));
        Self::JoinSource { base_alias }
    }

    /// Construct a qualifier for a structural alias — one that is locally
    /// determined and correct by construction (e.g., "agg_0", a loop variable,
    /// an extracted table name).
    ///
    /// For scope-derived qualification (CTE/join context), use `qualify_column()`.
    /// This is the one chokepoint where strings become qualifiers in transformer code.
    pub(in crate::pipeline::transformer_v3) fn structural(
        name: impl Into<String>,
    ) -> ColumnQualifier {
        ColumnQualifier::table(name, &QualifierMint(()))
    }

    /// Structural variant for schema-qualified table references.
    pub(in crate::pipeline::transformer_v3) fn structural_schema_table(
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> ColumnQualifier {
        ColumnQualifier::schema_table(schema, table, &QualifierMint(()))
    }
}
