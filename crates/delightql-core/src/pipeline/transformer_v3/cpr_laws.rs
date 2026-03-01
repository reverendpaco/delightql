use crate::pipeline::ast_addressed;

/// Structure to hold CPR law detection results
pub(crate) struct CprLawDetection {
    pub(crate) law1_qualified_columns: bool,
    #[allow(dead_code)]
    pub(crate) law2_alias_references: bool,
    #[allow(dead_code)]
    pub(crate) law3_forward_references: bool,
}

/// Common logic for detecting CPR laws from the CprSchema
/// This uses information from the resolver rather than re-walking expressions
pub(crate) fn detect_cpr_laws(cpr_schema: &ast_addressed::CprSchema) -> CprLawDetection {
    // Extract column metadata from the schema
    let columns = match cpr_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols,
        ast_addressed::CprSchema::Failed {
            resolved_columns, ..
        } => resolved_columns,
        ast_addressed::CprSchema::Unresolved(_) => {
            // If unresolved, we can't detect laws - return safe defaults
            return CprLawDetection {
                law1_qualified_columns: false,
                law2_alias_references: false,
                law3_forward_references: false,
            };
        }
        ast_addressed::CprSchema::Unknown => {
            // If unknown schema, we can't detect laws - return safe defaults
            return CprLawDetection {
                law1_qualified_columns: false,
                law2_alias_references: false,
                law3_forward_references: false,
            };
        }
    };

    // Law 1: Check if any columns were qualified in the source
    // The resolver sets was_qualified when it sees qualified references
    let law1_qualified_columns = columns
        .iter()
        .any(|col| col.info.is_qualified().unwrap_or(false));

    // Law 2: Check if WHERE clause references projection aliases
    // This is detected by the transformer when building WHERE conditions
    // that reference columns with aliases from the projection
    // NOTE: This detection happens at WHERE clause construction time,
    // not here in the pipe operator
    let law2_alias_references = false; // Detected elsewhere

    // Law 3: Check for forward references in projections
    // This would require analyzing if expressions reference aliases
    // defined later in the same projection list
    // NOTE: This should be detected by analyzing the expressions themselves
    let law3_forward_references = false; // TODO: Implement when needed

    CprLawDetection {
        law1_qualified_columns,
        law2_alias_references,
        law3_forward_references,
    }
}
