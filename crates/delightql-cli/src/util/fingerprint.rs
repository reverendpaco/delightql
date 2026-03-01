use anyhow::Result;
use delightql_backends::QueryResults;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
/// Result set fingerprinting for semantic equality testing
///
/// This module provides deterministic fingerprinting of query results
/// that is order-independent, allowing for semantic comparison of queries.
use std::path::Path;

/// A fingerprint of query results and database state
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultFingerprint {
    /// Hash of the result data only (order-independent, no column names)
    pub data_hash: String,
    /// Hash of the result set including column names (order-independent)
    pub result_hash: String,
    /// Hash of the database file
    pub db_hash: String,
    /// Combined hash of results and database
    pub combined_hash: String,
    /// Number of rows in the result
    pub row_count: usize,
    /// Number of columns in the result
    pub column_count: usize,
    /// Column names in order
    pub columns: Vec<String>,
}

/// JSON output format for fingerprints
#[derive(Debug, Serialize, Deserialize)]
pub struct FingerprintJson {
    pub dbhash: String,
    pub datahash: String,   // Data only (no column names)
    pub tablehash: String,  // Schema + data
    pub resulthash: String, // Legacy - same as tablehash for compatibility
    pub dimensions: String,
    pub totalhash: String,
    pub columns: Vec<String>, // Column names in order
}

impl ResultFingerprint {
    /// Create a fingerprint from query results and database path
    pub fn from_results(results: &QueryResults, db_path: Option<&Path>) -> Result<Self> {
        // 1. Hash each row individually
        let mut row_hashes: Vec<String> = Vec::with_capacity(results.rows.len());

        for row in &results.rows {
            let mut hasher = Sha256::new();

            for value in row {
                // Hash each string value
                // Since all values are strings in QueryResults, we hash them directly
                // Empty string represents NULL in the current implementation
                if value.is_empty() {
                    hasher.update(b"NULL");
                } else {
                    hasher.update(value.as_bytes());
                }
                hasher.update(b"|"); // Column separator
            }

            let row_hash = format!("{:x}", hasher.finalize());
            row_hashes.push(row_hash);
        }

        // 2. Sort row hashes to make order-independent
        row_hashes.sort();

        // 3a. Create data-only hash (no column names)
        let mut data_hasher = Sha256::new();
        data_hasher.update(b"ROWS:");
        for row_hash in &row_hashes {
            data_hasher.update(row_hash.as_bytes());
            data_hasher.update(b"\n");
        }
        let data_hash = format!("{:x}", data_hasher.finalize());

        // 3b. Create schema+data hash (includes column names)
        let mut result_hasher = Sha256::new();

        // Include column metadata in the hash
        result_hasher.update(b"COLUMNS:");
        for col in &results.columns {
            result_hasher.update(col.as_bytes());
            result_hasher.update(b"|");
        }
        result_hasher.update(b"\n");

        // Include sorted row hashes
        result_hasher.update(b"ROWS:");
        for row_hash in &row_hashes {
            result_hasher.update(row_hash.as_bytes());
            result_hasher.update(b"\n");
        }

        let result_hash = format!("{:x}", result_hasher.finalize());

        // 4. Hash the database file (if provided)
        let db_hash = if let Some(path) = db_path {
            hash_file(path).unwrap_or_else(|_| "ERROR".to_string())
        } else {
            "NO_DB".to_string()
        };

        // 5. Combine everything for final fingerprint
        let mut final_hasher = Sha256::new();
        final_hasher.update(b"RESULT:");
        final_hasher.update(result_hash.as_bytes());
        final_hasher.update(b"|DB:");
        final_hasher.update(db_hash.as_bytes());
        let combined_hash = format!("{:x}", final_hasher.finalize());

        Ok(Self {
            data_hash,
            result_hash,
            db_hash,
            combined_hash,
            row_count: results.rows.len(),
            column_count: results.columns.len(),
            columns: results.columns.clone(),
        })
    }

    /// Create a fingerprint without database hash (for pure result comparison)
    #[allow(dead_code)]
    pub fn from_results_only(results: &QueryResults) -> Result<Self> {
        Self::from_results(results, None)
    }

    /// Format the fingerprint for display
    #[allow(dead_code)]
    pub fn format_display(&self) -> String {
        format!(
            "Fingerprint:\n  \
             Combined: {}\n  \
             Results:  {} ({}x{} table)\n  \
             Database: {}",
            &self.combined_hash[..16], // Show first 16 chars for readability
            &self.result_hash[..16],
            self.row_count,
            self.column_count,
            if self.db_hash == "NO_DB" {
                "N/A".to_string()
            } else {
                self.db_hash[..16].to_string()
            }
        )
    }

    /// Convert to JSON format
    pub fn to_json(&self) -> FingerprintJson {
        FingerprintJson {
            dbhash: self.db_hash.clone(),
            datahash: self.data_hash.clone(),
            tablehash: self.result_hash.clone(),
            resulthash: self.result_hash.clone(), // Keep for backward compatibility
            dimensions: format!("{}x{}", self.row_count, self.column_count),
            totalhash: self.combined_hash.clone(),
            columns: self.columns.clone(),
        }
    }

    /// Convert to JSON string
    #[allow(dead_code)]
    pub fn to_json_string(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.to_json())?)
    }

    /// Convert to pretty JSON string
    pub fn to_json_pretty(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(&self.to_json())?)
    }
}

/// Hash a file using SHA256
fn hash_file(path: &Path) -> Result<String> {
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 8192];

    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_order_independence() {
        // Create two result sets with same data but different order
        let results1 = QueryResults {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec!["1".to_string(), "Alice".to_string()],
                vec!["2".to_string(), "Bob".to_string()],
            ],
            row_count: 2,
        };

        let results2 = QueryResults {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec!["2".to_string(), "Bob".to_string()],
                vec!["1".to_string(), "Alice".to_string()],
            ],
            row_count: 2,
        };

        let fp1 = ResultFingerprint::from_results_only(&results1).unwrap();
        let fp2 = ResultFingerprint::from_results_only(&results2).unwrap();

        // Should have same fingerprint despite different order
        assert_eq!(fp1.result_hash, fp2.result_hash);
        assert_eq!(fp1.combined_hash, fp2.combined_hash);
    }

    #[test]
    fn test_fingerprint_different_data() {
        let results1 = QueryResults {
            columns: vec!["id".to_string()],
            rows: vec![vec!["1".to_string()]],
            row_count: 1,
        };

        let results2 = QueryResults {
            columns: vec!["id".to_string()],
            rows: vec![vec!["2".to_string()]],
            row_count: 1,
        };

        let fp1 = ResultFingerprint::from_results_only(&results1).unwrap();
        let fp2 = ResultFingerprint::from_results_only(&results2).unwrap();

        // Should have different fingerprints
        assert_ne!(fp1.result_hash, fp2.result_hash);
        assert_ne!(fp1.combined_hash, fp2.combined_hash);
    }

    #[test]
    fn test_fingerprint_null_handling() {
        let results = QueryResults {
            columns: vec!["value".to_string()],
            rows: vec![
                vec!["".to_string()], // Empty string represents NULL
                vec!["not empty".to_string()],
            ],
            row_count: 2,
        };

        let fp = ResultFingerprint::from_results_only(&results).unwrap();

        // Should successfully fingerprint with empty strings (NULLs)
        assert!(!fp.result_hash.is_empty());
        assert_eq!(fp.row_count, 2);
    }
}
