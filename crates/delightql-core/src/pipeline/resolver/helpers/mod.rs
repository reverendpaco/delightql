// Helper functions for the resolver
// These are extracted to reduce the size of the main resolver module

pub(super) mod converters;
pub(crate) mod extraction;
pub(super) mod inner_cpr;

// Re-export for convenience within resolver
pub(super) use extraction::*;
pub(super) use inner_cpr::*;
