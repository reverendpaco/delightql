//! Predicates Cartridge - Built-in sigma predicates
//!
//! This cartridge provides constraint-oriented sigma predicates that represent
//! conceptually infinite relations and require EXISTS semantics (+ prefix):
//!
//! - `+like(value, pattern)` - SQL LIKE pattern matching
//! - `\+like(value, pattern)` - SQL NOT LIKE pattern matching
//!
//! Future predicates:
//! - `+=(x, y)`, `+<(x, y)`, `+>(x, y)` - Comparison operators
//! - `+regex(value, pattern)` - Regular expression matching
//!
//! The predicates cartridge is registered in the `std::predicates` namespace
//! and is marked as universal, making these predicates available everywhere
//! without needing an explicit `enlist!()`.

mod between;
mod like;

pub use between::BetweenPredicate;
pub use like::LikePredicate;

use super::{BinCartridge, BinCartridgeMetadata, BinEntity};
use crate::enums::Language;
use std::sync::Arc;

/// Predicates cartridge - provides sigma predicates
pub struct PredicatesCartridge;

impl BinCartridge for PredicatesCartridge {
    fn metadata(&self) -> BinCartridgeMetadata {
        BinCartridgeMetadata {
            source_uri: "bootstrap://predicates".to_string(),
            namespace_path: "std::predicates".to_string(),
            is_universal: true, // Available everywhere without borrow
            language: Language::DqlStandard,
            _description: Some("Constraint predicates (like, =, <, etc.)".to_string()),
        }
    }

    fn entities(&self) -> Vec<Arc<dyn BinEntity>> {
        vec![
            Arc::new(LikePredicate) as Arc<dyn BinEntity>,
            Arc::new(BetweenPredicate) as Arc<dyn BinEntity>,
            // Future: EqualsPredicate, LessThanPredicate, etc.
        ]
    }
}

/// Create a predicates cartridge instance
pub fn create_predicates_cartridge() -> Arc<dyn BinCartridge> {
    Arc::new(PredicatesCartridge)
}
