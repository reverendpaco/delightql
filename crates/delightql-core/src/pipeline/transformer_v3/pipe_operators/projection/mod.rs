// Projection operators: General, Modulo, ProjectOut, MetaIze
// Modularized to improve code navigability

mod general;
mod interior_drill_down;
mod meta_ize;
mod modulo;
pub(crate) mod narrowing_destructure;
mod pivot_support;
mod project_out;
pub(crate) mod tree_group_support;

// Re-export public API functions
pub use general::apply_general_projection;
pub use interior_drill_down::apply_interior_drill_down;
pub use meta_ize::apply_meta_ize;
pub use modulo::apply_modulo;
pub use project_out::apply_project_out;
