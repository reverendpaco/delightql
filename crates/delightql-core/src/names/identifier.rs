// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Position-valid authored names.
//!
//! A parsed spelling is a CANDIDATE, not yet an alias, a stage name, a
//! definition name, or a published column name. Each naming position has a
//! fallible constructor here, and the position types have private fields,
//! so a spelling reaches a semantic naming carrier only through the one
//! admission law:
//!
//! - exact `_` is reserved deixis, bare or stropped — stropping is
//!   spelling and does not release the reservation (strops-law);
//! - WORDS ARE NOT RESERVED: a keyword of DelightQL's own vocabulary or of
//!   any SQL target is an ordinary name wherever the grammar reached an
//!   identifier (top-grammar). Target keyword knowledge belongs to SQL
//!   emission, which quotes what a target would misread; it is never a
//!   source-admission veto, so no inventory of words lives here;
//! - a lawful strop is an ordinary exact name; its payload domain
//!   (emptiness, control characters, a backtick escape) is DOCKETED and
//!   deliberately not judged here — [`strop_payload`] is the one seam the
//!   future ruling lands in.
//!
//! Admission also RESERVES the authored spelling with the compilation
//! registry, which is what lets baptism refuse to draw an invented name
//! any authored name already owns (ALIAS ALWAYS PRE-EMPTS A MINT).
//!
//! The compiler's own exact spellings are a different source with a
//! different policy and never pass through this authority: a receipt
//! column the compiler spells is not an authored candidate. The two
//! non-authoring roads that re-read admitted text — stored definition
//! source and system-owned `_`-child blocks — skip the law at the
//! normalizer, which owns that classification.

use crate::diagnostic::Identifier;
use delightql_types::SqlIdentifier;

use crate::error::{DelightQLError, Result};

/// The naming position a candidate was written in — what the refusal
/// teaches with. Positions refuse identically today; the position is
/// carried so the teaching can say WHERE the unlawful name stood.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NamingPosition {
    /// A relation being referenced in query position.
    Reference,
    /// `as q` on a stage, member, head, or relation.
    StageOrAlias,
    /// `expr as name` in a publication or transform item.
    Published,
    /// A rename target (`*[old as new]`) or template survivor.
    RenameTarget,
    /// A definition or fact head name.
    Definition,
    /// A `let`-bound (CTE) name.
    Cte,
}

impl NamingPosition {
    fn role(self) -> &'static str {
        match self {
            NamingPosition::Reference => "a relation reference",
            NamingPosition::StageOrAlias => "an alias",
            NamingPosition::Published => "a published column name",
            NamingPosition::RenameTarget => "a rename target",
            NamingPosition::Definition => "a definition name",
            NamingPosition::Cte => "a binding name",
        }
    }
}

/// One admitted authored name. Private field: the only constructors are
/// the position admissions below, so holding one IS the proof that the
/// admission law ran.
#[derive(Clone, Debug, PartialEq)]
pub struct AuthoredName {
    spelling: SqlIdentifier,
}

/// The one admission law, shared by every naming position.
fn admit(
    spelling: SqlIdentifier,
    position: NamingPosition,
    registry: &crate::names::Registry,
) -> Result<AuthoredName> {
    if spelling.as_str() == "_" {
        return Err(DelightQLError::from(Identifier::Deixis {
            message: format!(
                "exact '_' is reserved for deixis; it cannot become {}",
                position.role()
            ),
        }));
    }
    if spelling.is_stropped() {
        strop_payload(spelling.as_str())?;
    }
    registry.reserve_authored(spelling.as_str(), spelling.is_stropped());
    Ok(AuthoredName { spelling })
}

/// The strop payload seam. WHAT TEXT A STROP MAY CONTAIN is docketed
/// (`DOCKET.md`): emptiness, control characters, and a backtick escape are
/// deliberately unruled, so this admits the grammar's current domain —
/// one or more non-backtick characters — and refuses nothing of its own.
/// The future ruling changes THIS function and nothing else.
fn strop_payload(_text: &str) -> Result<()> {
    Ok(())
}

macro_rules! position_name {
    ($(#[$doc:meta])* $name:ident, $position:expr) => {
        $(#[$doc])*
        #[derive(Clone, Debug, PartialEq)]
        pub struct $name(AuthoredName);

        impl $name {
            /// Admit an authored candidate into this position, reserving
            /// its spelling with the compilation registry.
            pub fn admit(
                spelling: SqlIdentifier,
                registry: &crate::names::Registry,
            ) -> Result<Self> {
                admit(spelling, $position, registry).map($name)
            }

            pub fn into_spelling(self) -> SqlIdentifier {
                self.0.spelling
            }
        }

        impl crate::lispy::ToLispy for $name {
            fn to_lispy(&self) -> String {
                self.0.spelling.to_lispy()
            }
        }
    };
}

position_name!(
    /// A relation name written in query position.
    ReferenceName,
    NamingPosition::Reference
);
position_name!(
    /// An `as` name on a stage, member, head, or relation — the answering
    /// name a scope will carry.
    StageName,
    NamingPosition::StageOrAlias
);
position_name!(
    /// A published column name (`expr as name`, a transform's target).
    PublishedName,
    NamingPosition::Published
);
position_name!(
    /// A rename target's literal new name.
    RenameName,
    NamingPosition::RenameTarget
);
position_name!(
    /// A definition or fact head's name.
    DefinitionName,
    NamingPosition::Definition
);
position_name!(
    /// A `let`-bound (CTE) name.
    CteName,
    NamingPosition::Cte
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::names::Registry;

    fn registry() -> Registry {
        Registry::new(&[])
    }

    #[test]
    fn exact_deixis_refuses_bare_and_stropped_in_every_position() {
        let reg = registry();
        for candidate in [SqlIdentifier::new("_"), SqlIdentifier::stropped("_")] {
            assert!(StageName::admit(candidate.clone(), &reg).is_err());
            assert!(PublishedName::admit(candidate.clone(), &reg).is_err());
            assert!(RenameName::admit(candidate.clone(), &reg).is_err());
            assert!(DefinitionName::admit(candidate.clone(), &reg).is_err());
            assert!(CteName::admit(candidate, &reg).is_err());
        }
    }

    #[test]
    fn keywords_of_either_vocabulary_are_ordinary_names_in_every_position() {
        let reg = registry();
        for word in ["as", "SELECT", "Where", "from", "in", "null", "double"] {
            for candidate in [SqlIdentifier::new(word), SqlIdentifier::stropped(word)] {
                assert_eq!(
                    StageName::admit(candidate.clone(), &reg)
                        .expect("no word is reserved")
                        .into_spelling()
                        .as_str(),
                    word
                );
                assert!(ReferenceName::admit(candidate.clone(), &reg).is_ok());
                assert!(PublishedName::admit(candidate.clone(), &reg).is_ok());
                assert!(RenameName::admit(candidate.clone(), &reg).is_ok());
                assert!(DefinitionName::admit(candidate.clone(), &reg).is_ok());
                assert!(CteName::admit(candidate, &reg).is_ok());
            }
        }
    }

    #[test]
    fn longer_underscore_spellings_are_ordinary_names() {
        let reg = registry();
        for word in ["__", "_____", "_fn"] {
            assert!(StageName::admit(SqlIdentifier::new(word), &reg).is_ok());
        }
    }

    #[test]
    fn admission_reserves_the_canonical_spelling() {
        let reg = registry();
        StageName::admit(SqlIdentifier::new("MyAlias"), &reg).unwrap();
        let reserved = reg.authored_reserved();
        assert_eq!(reserved.len(), 1);
        // The reservation is the CANONICAL identity: a later folded use of
        // the same name reserves nothing new.
        StageName::admit(SqlIdentifier::new("myalias"), &reg).unwrap();
        assert_eq!(reg.authored_reserved().len(), 1);
    }
}
