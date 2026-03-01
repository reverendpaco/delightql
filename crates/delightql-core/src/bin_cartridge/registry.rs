//! Bin Cartridge Registry
//!
//! The registry maintains an index of all registered bin cartridges and their entities,
//! providing fast O(1) lookup by entity name for the effect executor.

use super::{BinCartridge, BinEntity};
use std::collections::HashMap;
use std::sync::Arc;

/// Registry for bin cartridges and their entities
///
/// Maintains two indices:
/// 1. All cartridges (for iteration, lifecycle management)
/// 2. Entity name → entity mapping (for fast lookup during effect execution)
pub struct BinCartridgeRegistry {
    /// All registered cartridges (in registration order)
    cartridges: Vec<Arc<dyn BinCartridge>>,

    /// Entity name → entity mapping for fast lookup
    /// Key: entity name (e.g., "mount!", "enlist!")
    /// Value: The entity implementation (stored as Arc<dyn BinEntity>)
    ///
    /// We store Arc so the effect executor can clone the reference and release
    /// the registry borrow before executing.
    entity_index: HashMap<String, Arc<dyn BinEntity>>,
}

impl BinCartridgeRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            cartridges: Vec::new(),
            entity_index: HashMap::new(),
        }
    }

    /// Register a cartridge and index its entities
    ///
    /// # Arguments
    ///
    /// * `cartridge` - The cartridge to register
    ///
    /// # Panics
    ///
    /// Panics if two entities have the same name (name collision).
    /// This is a programming error that should be caught during development.
    pub fn register_cartridge(&mut self, cartridge: Arc<dyn BinCartridge>) {
        // Index all entities by name
        for entity in cartridge.entities() {
            let name = entity.name().to_string();

            if self.entity_index.contains_key(&name) {
                panic!(
                    "Entity name collision: '{}' is already registered. \
                     Each bin entity must have a unique name.",
                    name
                );
            }

            self.entity_index.insert(name, entity);
        }

        self.cartridges.push(cartridge);
    }

    /// Look up an entity by name
    ///
    /// Returns `Some(Arc<dyn BinEntity>)` if found, `None` if not registered.
    ///
    /// This is the primary lookup method used by the effect executor.
    /// Returns an Arc clone so the caller can release the registry borrow.
    ///
    /// Caller should downcast to the desired execution trait (e.g., EffectExecutable)
    /// using methods like `as_effect_executable()`.
    pub fn lookup_entity(&self, name: &str) -> Option<Arc<dyn BinEntity>> {
        self.entity_index.get(name).cloned()
    }

    /// Look up an entity by namespace-qualified name
    ///
    /// Constructs a qualified key from namespace path + entity name
    /// (e.g., ["sys", "execution"] + "compile" → "sys::execution.compile")
    pub fn lookup_qualified_entity(
        &self,
        namespace_path: &[&str],
        name: &str,
    ) -> Option<Arc<dyn BinEntity>> {
        if namespace_path.is_empty() {
            return self.lookup_entity(name);
        }
        let qualified = format!("{}.{}", namespace_path.join("::"), name);
        self.entity_index.get(&qualified).cloned()
    }

    /// Get all registered cartridges
    ///
    /// Used for lifecycle management (calling on_registered, on_shutdown)
    /// and syncing to bootstrap database.
    pub fn cartridges(&self) -> &[Arc<dyn BinCartridge>] {
        &self.cartridges
    }

}

impl Default for BinCartridgeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bin_cartridge::{
        BinCartridgeMetadata, EffectExecutable, EntityResult, EntitySignature, OutputSchema,
        Parameter,
    };
    use crate::enums::{EntityType, Language};
    use crate::error::Result;
    use crate::pipeline::asts::unresolved::DomainExpression;

    // Mock cartridge for testing
    struct TestCartridge;

    impl BinCartridge for TestCartridge {
        fn metadata(&self) -> BinCartridgeMetadata {
            BinCartridgeMetadata {
                source_uri: "test://mock".to_string(),
                namespace_path: "test".to_string(),
                is_universal: false,
                language: Language::DqlStandard,
                _description: Some("Test cartridge".to_string()),
            }
        }

        fn entities(&self) -> Vec<Arc<dyn BinEntity>> {
            vec![Arc::new(TestEntity)]
        }
    }

    // Mock entity for testing
    struct TestEntity;

    impl BinEntity for TestEntity {
        fn name(&self) -> &str {
            "test!"
        }

        fn entity_type(&self) -> EntityType {
            EntityType::BinPseudoPredicate
        }

        fn signature(&self) -> EntitySignature {
            EntitySignature {
                parameters: vec![],
                output_schema: OutputSchema::Relation(vec![]),
            }
        }

        fn has_side_effects(&self) -> bool {
            true
        }
    }

    impl EffectExecutable for TestEntity {
        fn execute(
            &self,
            _arguments: &[DomainExpression],
            _alias: Option<String>,
            _system: &mut crate::system::DelightQLSystem,
        ) -> Result<EntityResult> {
            unimplemented!("Test entity doesn't need real execution")
        }
    }

    #[test]
    fn test_register_and_lookup() {
        let mut registry = BinCartridgeRegistry::new();
        let cartridge = Arc::new(TestCartridge);

        registry.register_cartridge(cartridge);

        // Should be able to look up the entity
        assert!(registry.lookup_entity("test!").is_some());

        // Should return None for non-existent entity
        assert!(registry.lookup_entity("nonexistent!").is_none());
    }

    #[test]
    fn test_counts() {
        let mut registry = BinCartridgeRegistry::new();
        assert_eq!(registry.cartridge_count(), 0);
        assert_eq!(registry.entity_count(), 0);

        registry.register_cartridge(Arc::new(TestCartridge));

        assert_eq!(registry.cartridge_count(), 1);
        assert_eq!(registry.entity_count(), 1);
    }

    #[test]
    #[should_panic(expected = "Entity name collision")]
    fn test_name_collision_panics() {
        let mut registry = BinCartridgeRegistry::new();

        // Register same cartridge twice - should panic on second registration
        registry.register_cartridge(Arc::new(TestCartridge));
        registry.register_cartridge(Arc::new(TestCartridge));
    }
}
