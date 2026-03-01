//! Prelude Cartridge - Built-in pseudo-predicates
//!
//! This cartridge provides the core pseudo-predicates that are available
//! by default in all DelightQL sessions:
//!
//! - `mount!(db_path, namespace)` - Mount a database
//! - `enlist!(namespace)` - Enable unqualified access to namespace entities
//! - `delist!(namespace)` - Remove unqualified access to namespace entities
//! - `run!(file_path)` - Execute queries from a file
//!
//! The prelude cartridge is registered in the `std::prelude` namespace
//! and is marked as universal, making these predicates available everywhere
//! without needing an explicit `enlist!()`.

mod alias;
mod compile;
pub(crate) mod consult;
mod consult_tree;
mod delist;
mod enlist;
mod ground;
mod imprint;
mod mount;
mod reconsult;
mod refresh;
mod run;
mod unconsult;
mod unmount;

pub use alias::AliasPredicate;
pub use compile::CompilePredicate;
pub use consult::ConsultPredicate;
pub use consult_tree::ConsultTreePredicate;
pub use delist::DelistPredicate;
pub use enlist::EnlistPredicate;
pub use ground::GroundPredicate;
pub use imprint::ImprintPredicate;
pub use mount::MountPredicate;
pub use reconsult::ReconsultPredicate;
pub use refresh::RefreshPredicate;
pub use run::RunPredicate;
pub use unconsult::UnconsultPredicate;
pub use unmount::UnmountPredicate;

use super::{BinCartridge, BinCartridgeMetadata, BinEntity};
use crate::enums::Language;
use crate::pipeline::asts::unresolved::*;
use std::sync::Arc;

/// Create a single-column directive result: _(namespace @ "ns")
///
/// All namespace-producing directives (mount!, consult!, enlist!, delist!, ground!)
/// return this uniform schema for pipe composition.
pub(crate) fn directive_result(namespace: &str, alias: Option<String>) -> Relation {
    let headers = vec![DomainExpression::lvar_builder("ns".to_string()).build()];
    let row = Row {
        values: vec![DomainExpression::Literal {
            value: LiteralValue::String(namespace.to_string()),
            alias: None,
        }],
    };
    Relation::Anonymous {
        column_headers: Some(headers),
        rows: vec![row],
        alias: alias.map(|s| s.into()),
        outer: false,
        exists_mode: false,
        qua_target: None,
        cpr_schema: PhaseBox::phantom(),
    }
}

/// Prelude cartridge - provides core pseudo-predicates
pub struct PreludeCartridge;

impl BinCartridge for PreludeCartridge {
    fn metadata(&self) -> BinCartridgeMetadata {
        BinCartridgeMetadata {
            source_uri: "bootstrap://prelude".to_string(),
            namespace_path: "std::prelude".to_string(),
            is_universal: true, // Available everywhere without enlist
            language: Language::DqlStandard,
            _description: Some("Built-in pseudo-predicates for state manipulation".to_string()),
        }
    }

    fn entities(&self) -> Vec<Arc<dyn BinEntity>> {
        vec![
            Arc::new(MountPredicate) as Arc<dyn BinEntity>,
            Arc::new(EnlistPredicate) as Arc<dyn BinEntity>,
            Arc::new(DelistPredicate) as Arc<dyn BinEntity>,
            Arc::new(RunPredicate) as Arc<dyn BinEntity>,
            Arc::new(ConsultPredicate) as Arc<dyn BinEntity>,
            Arc::new(ConsultTreePredicate) as Arc<dyn BinEntity>,
            Arc::new(GroundPredicate) as Arc<dyn BinEntity>,
            Arc::new(ImprintPredicate) as Arc<dyn BinEntity>,
            Arc::new(AliasPredicate) as Arc<dyn BinEntity>,
            Arc::new(UnmountPredicate) as Arc<dyn BinEntity>,
            Arc::new(UnconsultPredicate) as Arc<dyn BinEntity>,
            Arc::new(RefreshPredicate) as Arc<dyn BinEntity>,
            Arc::new(ReconsultPredicate) as Arc<dyn BinEntity>,
            Arc::new(CompilePredicate) as Arc<dyn BinEntity>,
        ]
    }
}

/// Create a prelude cartridge instance
pub fn create_prelude_cartridge() -> Arc<dyn BinCartridge> {
    Arc::new(PreludeCartridge)
}
