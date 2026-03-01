// Aggregation operators: AggregatePipe

use crate::error::{DelightQLError, Result};
use crate::pipeline::sql_ast_v3::{SelectBuilder, SelectStatement};

/// Handle AggregatePipe operator: |~>
pub fn apply_aggregate_pipe(_builder: SelectBuilder) -> Result<SelectStatement> {
    Err(DelightQLError::not_implemented(
        "AggregatePipe operator (|~>)",
    ))
}
