use delightql_types::Result;
/// Database Schema Abstraction
///
/// This module provides a database-agnostic interface for accessing schema information.
/// The AST resolver uses this trait to validate semantic references without knowing
/// which specific database backend is being used.
use std::collections::HashMap;

/// Information about a database column
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
}

/// Information about a database table
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

/// Database schema containing all tables
#[derive(Debug, Clone)]
pub struct DatabaseSchema {
    pub tables: HashMap<String, TableInfo>,
}

impl Default for DatabaseSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseSchema {
    /// Create a new empty schema
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Add a table to the schema
    pub fn add_table(&mut self, table: TableInfo) {
        self.tables.insert(table.name.clone(), table);
    }

    /// Get a table by name
    pub fn get_table(&self, name: &str) -> Option<&TableInfo> {
        self.tables.get(name)
    }

    /// Get all table names
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Check if a table exists
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

/// Trait for providing schema information to the AST resolver
/// This abstraction allows the resolver to work with any database backend
pub trait SchemaProvider: Send + Sync {
    /// Get the complete database schema
    fn get_schema(&self) -> Result<DatabaseSchema>;

    /// Get information about a specific table
    fn get_table_info(&self, table_name: &str) -> Result<TableInfo>;

    /// Check if a table exists
    fn table_exists(&self, table_name: &str) -> Result<bool>;

    /// Get all table names in the database
    fn list_tables(&self) -> Result<Vec<String>>;
}
