// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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

    /// (namespace fq, local entity name) → entity.
    ///
    /// CANONICAL IDENTITY: registry
    /// keys contain namespace plus local entity name. A built-in cannot
    /// become globally callable merely because its local name happens to
    /// contain `::` — the old single-string index allowed exactly that
    /// bypass for `sys::execution.compile`.
    ///
    /// We store Arc so the effect executor can clone the reference and release
    /// the registry borrow before executing.
    entity_index: HashMap<(String, String), Arc<dyn BinEntity>>,

    /// Namespaces whose entities are visible UNQUALIFIED, in registration
    /// order. Today this is the universal cartridges (std::prelude,
    /// std::predicates); enlisted/unqualified access works only when
    /// visibility permits.
    universal_namespaces: Vec<String>,
}

impl BinCartridgeRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            cartridges: Vec::new(),
            entity_index: HashMap::new(),
            universal_namespaces: Vec::new(),
        }
    }

    /// Register a cartridge and index its entities under their namespace
    /// identity: the entity's `namespace_override()` if declared, else the
    /// cartridge's namespace path.
    ///
    /// # Panics
    ///
    /// Panics if two entities share one (namespace, name) identity.
    /// This is a programming error that should be caught during development.
    pub fn register_cartridge(&mut self, cartridge: Arc<dyn BinCartridge>) {
        let metadata = cartridge.metadata();
        for entity in cartridge.entities() {
            let namespace = entity
                .namespace_override()
                .unwrap_or(&metadata.namespace_path)
                .to_string();
            let name = entity.name().to_string();

            if self
                .entity_index
                .insert((namespace.clone(), name.clone()), entity)
                .is_some()
            {
                panic!(
                    "Entity identity collision: '{}' in namespace '{}' is \
                     already registered. Each bin entity must have a unique \
                     (namespace, name) identity.",
                    name, namespace
                );
            }
        }

        if metadata.is_universal && !self.universal_namespaces.contains(&metadata.namespace_path) {
            self.universal_namespaces
                .push(metadata.namespace_path.clone());
        }

        self.cartridges.push(cartridge);
    }

    /// Look up an entity by UNQUALIFIED name through the visibility rule:
    /// only entities whose identity namespace belongs to a universal
    /// cartridge are reachable without qualification. An entity carrying a
    /// `namespace_override` outside those namespaces (e.g. compile under
    /// `sys::execution`) is NOT unqualified-reachable.
    ///
    /// Returns an Arc clone so the caller can release the registry borrow.
    pub fn lookup_entity(&self, name: &str) -> Option<Arc<dyn BinEntity>> {
        self.universal_namespaces
            .iter()
            .find_map(|ns| self.entity_index.get(&(ns.clone(), name.to_string())))
            .cloned()
    }

    /// Look up an entity by its namespace-qualified identity
    /// (e.g., ["sys", "execution"] + "compile").
    pub fn lookup_qualified_entity<S: AsRef<str>>(
        &self,
        namespace_path: &[S],
        name: &str,
    ) -> Option<Arc<dyn BinEntity>> {
        if namespace_path.is_empty() {
            return self.lookup_entity(name);
        }
        let namespace = namespace_path
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>()
            .join("::");
        self.entity_index
            .get(&(namespace, name.to_string()))
            .cloned()
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

        // TestCartridge is NOT universal: unqualified visibility refuses...
        assert!(registry.lookup_entity("test!").is_none());
        // ...while the (namespace, name) identity resolves qualified.
        assert!(registry
            .lookup_qualified_entity(&["test"], "test!")
            .is_some());

        // Should return None for non-existent identities
        assert!(registry
            .lookup_qualified_entity(&["test"], "nonexistent!")
            .is_none());
        assert!(registry
            .lookup_qualified_entity(&["other"], "test!")
            .is_none());
    }

    #[test]
    fn test_counts() {
        let mut registry = BinCartridgeRegistry::new();
        assert_eq!(registry.cartridges().len(), 0);
        assert!(registry
            .lookup_qualified_entity(&["test"], "test!")
            .is_none());

        registry.register_cartridge(Arc::new(TestCartridge));

        assert_eq!(registry.cartridges().len(), 1);
        assert!(registry
            .lookup_qualified_entity(&["test"], "test!")
            .is_some());
    }

    #[test]
    #[should_panic(expected = "Entity identity collision")]
    fn test_name_collision_panics() {
        let mut registry = BinCartridgeRegistry::new();

        // Register same cartridge twice - should panic on second registration
        registry.register_cartridge(Arc::new(TestCartridge));
        registry.register_cartridge(Arc::new(TestCartridge));
    }

    #[test]
    fn universal_visibility_governs_unqualified_lookup() {
        struct UniversalCartridge;
        impl BinCartridge for UniversalCartridge {
            fn metadata(&self) -> BinCartridgeMetadata {
                BinCartridgeMetadata {
                    source_uri: "test://universal".to_string(),
                    namespace_path: "std::testuniv".to_string(),
                    is_universal: true,
                    language: Language::DqlStandard,
                    _description: None,
                }
            }
            fn entities(&self) -> Vec<Arc<dyn BinEntity>> {
                vec![Arc::new(TestEntity)]
            }
        }
        let mut registry = BinCartridgeRegistry::new();
        registry.register_cartridge(Arc::new(UniversalCartridge));
        // Universal namespace: unqualified AND qualified both resolve.
        assert!(registry.lookup_entity("test!").is_some());
        assert!(registry
            .lookup_qualified_entity(&["std", "testuniv"], "test!")
            .is_some());
    }
}
