// CFE Precompilation - Convert unresolved CFE definitions to refined, ready-to-substitute form
//
// This module implements the "precompilation" step where CFE bodies are:
// 1. Resolved (with parameters as fake columns)
// 2. Refined (including embedded subqueries)
// 3. Post-processed (parameter Lvars → Parameter nodes)
//
// See CFE_IMPLEMENTATION_DESIGN.md for detailed rationale.

pub(crate) mod definition;
pub(crate) mod postprocessing;
mod provenance;
mod refining;

// Public API - the main entry point for CFE precompilation
pub use definition::precompile_query_cfes;
