//! Resolution subsystem for DelightQL
//!
//! This module provides entity tracking and resolution services to the pipeline.
//! EntityRegistry consolidates schema lookups, CTEs, and namespace resolution
//! into a single registry.

pub mod entity;
pub mod registry;
pub mod resolver;

pub use entity::*;
pub use registry::*;
pub use resolver::*;
