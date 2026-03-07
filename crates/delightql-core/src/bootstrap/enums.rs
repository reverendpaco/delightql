//! Bootstrap enum definitions - SINGLE SOURCE OF TRUTH
//!
//! All enum values for bootstrap metadata tables are defined here.
//! Database tables are seeded programmatically from these definitions
//! during bootstrap initialization.
//!
//! This module replaces the previous approach where enum values were defined
//! in both seed.sql (as INSERT statements) and Rust code (as magic numbers).
//! Now Rust enums are the single source of truth, providing:
//! - Type safety: Compiler enforces correct usage
//! - No sync issues: Database seeded from these definitions
//! - Self-documenting: Clear enum names instead of magic numbers
//! - Refactor-safe: Compiler finds all usages

use anyhow::{anyhow, Result};

// =============================================================================
// Source Type Enum
// =============================================================================

/// Source type for cartridges
///
/// Corresponds to `source_type_enum` table in bootstrap database.
///
/// Determines where a cartridge's entities originate from:
/// - File: Text files with DQL/SQL source code
/// - FileBin: Binary/serialized definitions
/// - Db: Database connections (introspected tables/views)
/// - Bin: Built-in entities compiled into the DelightQL engine
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum SourceType {
    /// Text files containing DQL/SQL source code
    File = 1,

    /// Binary files (compiled/serialized definitions)
    FileBin = 2,

    /// Database connection cartridge (e.g., sqlite://data.db)
    /// Entities are discovered via introspection
    Db = 3,

    /// Built-in binary cartridge (compiled into DelightQL)
    /// Entities have native Rust implementations
    Bin = 4,
}

impl SourceType {
    /// All source type variants (for iteration during seeding)
    pub const ALL: &'static [Self] = &[Self::File, Self::FileBin, Self::Db, Self::Bin];

    /// Convert to integer for database storage
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    /// Get variant name for database
    pub fn variant_name(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::FileBin => "filebin",
            Self::Db => "db",
            Self::Bin => "bin",
        }
    }

    /// Get human-readable explanation for database
    pub fn explanation(self) -> &'static str {
        match self {
            Self::File => "Text files containing DQL/SQL source code",
            Self::FileBin => "Binary files (compiled/serialized definitions)",
            Self::Db => {
                "Entities coming from a database connection. Almost always just tables and views"
            }
            Self::Bin => "Entities that are defined in the code of the delightful engine itself",
        }
    }

    /// Parse from integer (for reading from database)
    pub fn from_i32(value: i32) -> Result<Self> {
        match value {
            1 => Ok(Self::File),
            2 => Ok(Self::FileBin),
            3 => Ok(Self::Db),
            4 => Ok(Self::Bin),
            _ => Err(anyhow!("Invalid source_type_enum value: {}", value)),
        }
    }
}

// =============================================================================
// Language Enum
// =============================================================================

/// Programming language for cartridges
///
/// Corresponds to `language` table in bootstrap database.
///
/// Represents the language/dialect combination for entities in a cartridge.
/// Each variant specifies both the base language and the specific dialect/version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum Language {
    /// DelightQL standard (version 1.0)
    DqlStandard = 1,

    /// SQL with PostgreSQL dialect (version 16.3)
    SqlPostgres = 2,

    /// SQL with SQLite dialect (version 3.45.0)
    SqlSqlite = 3,
}

impl Language {
    /// All language variants (for iteration during seeding)
    pub const ALL: &'static [Self] = &[Self::DqlStandard, Self::SqlPostgres, Self::SqlSqlite];

    /// Convert to integer for database storage
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    /// Get base language name
    pub fn language(self) -> &'static str {
        match self {
            Self::DqlStandard => "DQL",
            Self::SqlPostgres => "SQL",
            Self::SqlSqlite => "SQL",
        }
    }

    /// Get dialect name
    pub fn dialect(self) -> &'static str {
        match self {
            Self::DqlStandard => "standard",
            Self::SqlPostgres => "postgres",
            Self::SqlSqlite => "sqlite",
        }
    }

    /// Get version string
    pub fn version(self) -> &'static str {
        match self {
            Self::DqlStandard => "1.0",
            Self::SqlPostgres => "16.3",
            Self::SqlSqlite => "3.45.0",
        }
    }

    /// Parse from integer (for reading from database)
    pub fn from_i32(value: i32) -> Result<Self> {
        match value {
            1 => Ok(Self::DqlStandard),
            2 => Ok(Self::SqlPostgres),
            3 => Ok(Self::SqlSqlite),
            _ => Err(anyhow!("Invalid language value: {}", value)),
        }
    }
}

// =============================================================================
// Entity Type Enum
// =============================================================================

/// Entity type classification
///
/// Corresponds to `entity_type_enum` table in bootstrap database.
///
/// Classifies the different kinds of entities that can exist in DelightQL:
/// - DQL entities: Functions, views, tables defined in DelightQL syntax
/// - DB entities: Tables and views discovered from database introspection
/// - Higher-order entities: Functions that take other functions as arguments
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum EntityType {
    /// DelightQL function expression (standard function)
    DqlFunctionExpression = 1,

    /// DelightQL higher-order function expression (takes functions as args)
    DqlHoFunctionExpression = 2,

    /// DelightQL context-aware function expression
    DqlContextAwareFunctionExpression = 3,

    /// DelightQL temporary view expression
    DqlTemporaryViewExpression = 4,

    /// DelightQL permanent view expression
    DqlPermanentViewExpression = 5,

    /// DelightQL temporary table expression
    DqlTemporaryTableExpression = 6,

    /// DelightQL permanent table expression
    DqlPermanentTableExpression = 7,

    /// DelightQL higher-order temporary view expression
    DqlHoTemporaryViewExpression = 8,

    /// DelightQL temporary sigma rule
    DqlTemporarySigmaRule = 9,

    /// Database permanent table (discovered via introspection)
    DbPermanentTable = 10,

    /// Database permanent view (discovered via introspection)
    DbPermanentView = 11,

    /// Database temporary table (discovered via introspection)
    DbTemporaryTable = 12,

    /// Database temporary view (discovered via introspection)
    DbTemporaryView = 13,

    /// Built-in pseudo-predicate (import!, engage!, etc.)
    /// Pseudo-predicates are state-mutating relations with `!` suffix
    /// that execute at Phase 1.X and are compiled into the DelightQL engine
    BinPseudoPredicate = 14,

    /// Built-in sigma predicate (like(), =(), <(), etc.)
    /// Sigma predicates are constraint-oriented predicates representing
    /// conceptually infinite relations that require EXISTS semantics (+prefix)
    /// Examples: +like(email, "%@gmail.com"), +=(status, "active")
    BinSigmaPredicate = 15,

    /// DelightQL fact expression (inline data as VALUES)
    DqlFactExpression = 16,

    /// DelightQL ER-context rule (entity-relationship join rule)
    DqlErContextRule = 17,

    /// Formerly: companion table definition. Retired — value 18 reserved.
    DqlCompanionDefinition = 18,
}

impl EntityType {
    /// All entity type variants (for iteration during seeding)
    pub const ALL: &'static [Self] = &[
        Self::DqlFunctionExpression,
        Self::DqlHoFunctionExpression,
        Self::DqlContextAwareFunctionExpression,
        Self::DqlTemporaryViewExpression,
        Self::DqlPermanentViewExpression,
        Self::DqlTemporaryTableExpression,
        Self::DqlPermanentTableExpression,
        Self::DqlHoTemporaryViewExpression,
        Self::DqlTemporarySigmaRule,
        Self::DbPermanentTable,
        Self::DbPermanentView,
        Self::DbTemporaryTable,
        Self::DbTemporaryView,
        Self::BinPseudoPredicate,
        Self::BinSigmaPredicate,
        Self::DqlFactExpression,
        Self::DqlErContextRule,
        Self::DqlCompanionDefinition,
    ];

    /// Convert to integer for database storage
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    /// Get variant name for database
    pub fn variant_name(self) -> &'static str {
        match self {
            Self::DqlFunctionExpression => "DQLFunctionExpression",
            Self::DqlHoFunctionExpression => "DQLHOFunctionExpression",
            Self::DqlContextAwareFunctionExpression => "DQLContextAwareFunctionExpression",
            Self::DqlTemporaryViewExpression => "DQLTemporaryViewExpression",
            Self::DqlPermanentViewExpression => "DQLPermanentViewExpression",
            Self::DqlTemporaryTableExpression => "DQLTemporaryTableExpression",
            Self::DqlPermanentTableExpression => "DQLPermanentyTableExpression", // Note: preserves typo from seed.sql
            Self::DqlHoTemporaryViewExpression => "DQLHOTemporaryViewExpression",
            Self::DqlTemporarySigmaRule => "DQLTemporarySigmaRule",
            Self::DbPermanentTable => "DBPermanentTable",
            Self::DbPermanentView => "DBPermanentView",
            Self::DbTemporaryTable => "DBTemporaryTable",
            Self::DbTemporaryView => "DBTemporaryView",
            Self::BinPseudoPredicate => "BinPseudoPredicate",
            Self::BinSigmaPredicate => "BinSigmaPredicate",
            Self::DqlFactExpression => "DQLFactExpression",
            Self::DqlErContextRule => "DQLErContextRule",
            Self::DqlCompanionDefinition => "DQLCompanionDefinition",
        }
    }

    /// Whether this is a higher-order entity
    pub fn is_ho(self) -> bool {
        matches!(
            self,
            Self::DqlHoFunctionExpression | Self::DqlHoTemporaryViewExpression
        )
    }

    /// Whether this is a function entity
    pub fn is_fn(self) -> bool {
        matches!(
            self,
            Self::DqlFunctionExpression
                | Self::DqlHoFunctionExpression
                | Self::DqlContextAwareFunctionExpression
        )
    }

    /// Parse from integer (for reading from database)
    pub fn from_i32(value: i32) -> Result<Self> {
        match value {
            1 => Ok(Self::DqlFunctionExpression),
            2 => Ok(Self::DqlHoFunctionExpression),
            3 => Ok(Self::DqlContextAwareFunctionExpression),
            4 => Ok(Self::DqlTemporaryViewExpression),
            5 => Ok(Self::DqlPermanentViewExpression),
            6 => Ok(Self::DqlTemporaryTableExpression),
            7 => Ok(Self::DqlPermanentTableExpression),
            8 => Ok(Self::DqlHoTemporaryViewExpression),
            9 => Ok(Self::DqlTemporarySigmaRule),
            10 => Ok(Self::DbPermanentTable),
            11 => Ok(Self::DbPermanentView),
            12 => Ok(Self::DbTemporaryTable),
            13 => Ok(Self::DbTemporaryView),
            14 => Ok(Self::BinPseudoPredicate),
            15 => Ok(Self::BinSigmaPredicate),
            16 => Ok(Self::DqlFactExpression),
            17 => Ok(Self::DqlErContextRule),
            18 => Ok(Self::DqlCompanionDefinition),
            _ => Err(anyhow!("Invalid entity_type_enum value: {}", value)),
        }
    }
}

// =============================================================================
// Connection Type Enum
// =============================================================================

/// Connection type for database connections
///
/// Corresponds to `connection_type_enum` table in bootstrap database.
///
/// Specifies the type of database connection and how to establish it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ConnectionType {
    /// Direct connection to SQLite file
    SqliteFile = 1,

    /// Direct connection to in-memory SQLite database
    SqliteMemory = 2,

    /// IPC connection to PostgreSQL via dql-server daemon
    PostgresIpc = 3,

    /// Direct connection to DuckDB
    DuckDb = 4,

    /// Internal engine bootstrap metadata store
    Bootstrap = 5,
}

impl ConnectionType {
    /// All connection type variants (for iteration during seeding)
    pub const ALL: &'static [Self] = &[
        Self::SqliteFile,
        Self::SqliteMemory,
        Self::PostgresIpc,
        Self::DuckDb,
        Self::Bootstrap,
    ];

    /// Convert to integer for database storage
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    /// Get variant name for database
    pub fn variant_name(self) -> &'static str {
        match self {
            Self::SqliteFile => "sqlite-file",
            Self::SqliteMemory => "sqlite-memory",
            Self::PostgresIpc => "postgres-ipc",
            Self::DuckDb => "duckdb",
            Self::Bootstrap => "bootstrap",
        }
    }

    /// Get human-readable explanation for database
    pub fn explanation(self) -> &'static str {
        match self {
            Self::SqliteFile => "Direct connection to SQLite file",
            Self::SqliteMemory => "Direct connection to in-memory SQLite database",
            Self::PostgresIpc => "IPC connection to PostgreSQL via dql-server daemon",
            Self::DuckDb => "Direct connection to DuckDB",
            Self::Bootstrap => "Internal engine bootstrap metadata store",
        }
    }

    /// Parse from integer (for reading from database)
    pub fn from_i32(value: i32) -> Result<Self> {
        match value {
            1 => Ok(Self::SqliteFile),
            2 => Ok(Self::SqliteMemory),
            3 => Ok(Self::PostgresIpc),
            4 => Ok(Self::DuckDb),
            5 => Ok(Self::Bootstrap),
            _ => Err(anyhow!("Invalid connection_type_enum value: {}", value)),
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_type_roundtrip() {
        for st in SourceType::ALL {
            let id = st.as_i32();
            let parsed = SourceType::from_i32(id).unwrap();
            assert_eq!(*st, parsed);
        }
    }

    #[test]
    fn source_type_names() {
        assert_eq!(SourceType::File.variant_name(), "file");
        assert_eq!(SourceType::FileBin.variant_name(), "filebin");
        assert_eq!(SourceType::Db.variant_name(), "db");
        assert_eq!(SourceType::Bin.variant_name(), "bin");
    }

    #[test]
    fn language_roundtrip() {
        for lang in Language::ALL {
            let id = lang.as_i32();
            let parsed = Language::from_i32(id).unwrap();
            assert_eq!(*lang, parsed);
        }
    }

    #[test]
    fn language_components() {
        assert_eq!(Language::DqlStandard.language(), "DQL");
        assert_eq!(Language::DqlStandard.dialect(), "standard");
        assert_eq!(Language::DqlStandard.version(), "1.0");

        assert_eq!(Language::SqlSqlite.language(), "SQL");
        assert_eq!(Language::SqlSqlite.dialect(), "sqlite");
        assert_eq!(Language::SqlSqlite.version(), "3.45.0");
    }

    #[test]
    fn entity_type_roundtrip() {
        for et in EntityType::ALL {
            let id = et.as_i32();
            let parsed = EntityType::from_i32(id).unwrap();
            assert_eq!(*et, parsed);
        }
    }

    #[test]
    fn entity_type_metadata() {
        assert!(EntityType::DqlHoFunctionExpression.is_ho());
        assert!(EntityType::DqlHoTemporaryViewExpression.is_ho());
        assert!(!EntityType::DqlFunctionExpression.is_ho());

        assert!(EntityType::DqlFunctionExpression.is_fn());
        assert!(EntityType::DqlHoFunctionExpression.is_fn());
        assert!(EntityType::DqlContextAwareFunctionExpression.is_fn());
        assert!(!EntityType::DbPermanentTable.is_fn());
    }

    #[test]
    fn connection_type_roundtrip() {
        for ct in ConnectionType::ALL {
            let id = ct.as_i32();
            let parsed = ConnectionType::from_i32(id).unwrap();
            assert_eq!(*ct, parsed);
        }
    }

    #[test]
    fn no_duplicate_ids() {
        use std::collections::HashSet;

        // Ensure no accidental duplicate IDs in enums
        let source_type_ids: HashSet<i32> = SourceType::ALL.iter().map(|st| st.as_i32()).collect();
        assert_eq!(source_type_ids.len(), SourceType::ALL.len());

        let language_ids: HashSet<i32> = Language::ALL.iter().map(|l| l.as_i32()).collect();
        assert_eq!(language_ids.len(), Language::ALL.len());

        let entity_type_ids: HashSet<i32> = EntityType::ALL.iter().map(|et| et.as_i32()).collect();
        assert_eq!(entity_type_ids.len(), EntityType::ALL.len());

        let connection_type_ids: HashSet<i32> =
            ConnectionType::ALL.iter().map(|ct| ct.as_i32()).collect();
        assert_eq!(connection_type_ids.len(), ConnectionType::ALL.len());
    }
}
