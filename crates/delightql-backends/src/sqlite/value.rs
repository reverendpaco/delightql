/// Type-safe SQL value representation
///
/// Preserves type information for NULL values and provides proper display formatting

use std::fmt;

/// Represents a SQL value with proper type information
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    /// NULL value (no type information)
    Null,
    /// Integer value (i64)
    Integer(i64),
    /// Real/Float value (f64)
    Real(f64),
    /// Text/String value
    Text(String),
    /// Blob/Binary value
    Blob(Vec<u8>),
}

impl SqlValue {
    /// Check if this value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, SqlValue::Null)
    }
    
    /// Convert to display string for output formatting
    pub fn to_display_string(&self) -> String {
        match self {
            SqlValue::Null => "NULL".to_string(),
            SqlValue::Integer(i) => i.to_string(),
            SqlValue::Real(f) => f.to_string(),
            SqlValue::Text(s) => s.clone(),
            SqlValue::Blob(b) => format!("<blob:{} bytes>", b.len()),
        }
    }
    
    /// Convert from rusqlite Value
    pub fn from_rusqlite_value(row: &rusqlite::Row, index: usize) -> rusqlite::Result<Self> {
        use rusqlite::types::ValueRef;
        
        match row.get_ref(index)? {
            ValueRef::Null => Ok(SqlValue::Null),
            ValueRef::Integer(i) => Ok(SqlValue::Integer(i)),
            ValueRef::Real(f) => Ok(SqlValue::Real(f)),
            ValueRef::Text(s) => Ok(SqlValue::Text(String::from_utf8_lossy(s).to_string())),
            ValueRef::Blob(b) => Ok(SqlValue::Blob(b.to_vec())),
        }
    }
}

impl fmt::Display for SqlValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_display_string())
    }
}

/// Type-safe query result that preserves NULL type information
#[derive(Debug, Clone, PartialEq)]
pub struct TypedQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub affected_rows: Option<usize>,
}

impl TypedQueryResult {
    pub fn new(columns: Vec<String>, rows: Vec<Vec<SqlValue>>) -> Self {
        Self {
            columns,
            rows,
            affected_rows: None,
        }
    }
    
    pub fn with_affected_rows(mut self, affected: usize) -> Self {
        self.affected_rows = Some(affected);
        self
    }
    
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
    
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
    
    /// Convert to string-based result for backward compatibility
    pub fn to_string_result(&self) -> super::executor::QueryResult {
        let string_rows = self.rows.iter()
            .map(|row| row.iter().map(|val| val.to_display_string()).collect())
            .collect();
            
        super::executor::QueryResult {
            columns: self.columns.clone(),
            rows: string_rows,
            affected_rows: self.affected_rows,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_sql_value_display() {
        assert_eq!(SqlValue::Null.to_display_string(), "NULL");
        assert_eq!(SqlValue::Integer(42).to_display_string(), "42");
        assert_eq!(SqlValue::Real(3.14).to_display_string(), "3.14");
        assert_eq!(SqlValue::Text("hello".to_string()).to_display_string(), "hello");
        assert_eq!(SqlValue::Blob(vec![1, 2, 3]).to_display_string(), "<blob:3 bytes>");
    }
    
    #[test]
    fn test_null_checking() {
        assert!(SqlValue::Null.is_null());
        assert!(!SqlValue::Integer(0).is_null());
        assert!(!SqlValue::Text("".to_string()).is_null());
    }
}