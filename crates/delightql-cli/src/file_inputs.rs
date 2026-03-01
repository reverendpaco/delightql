//! File input tracking with auto-numbering for process substitutions
//!
//! Handles tracking file inputs for `dql tools munge` command, automatically
//! detecting process substitutions and assigning appropriate table names.

use crate::modifiers::CsvModifiers;
use std::path::{Path, PathBuf};

/// Represents a single file input with metadata
#[derive(Debug, Clone)]
pub struct FileInput {
    /// Original path provided by user
    pub path: PathBuf,

    /// Table name (derived or explicit)
    pub table_name: String,

    /// Format (json, csv, tsv, jsonl)
    pub format: FileFormat,

    /// Modifiers (for CSV/TSV)
    pub modifiers: Option<CsvModifiers>,

    /// Whether this is a process substitution
    pub is_pipe: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Json,
    Csv,
    Tsv,
    JsonL,
}

impl FileInput {
    /// Create a new file input, auto-detecting pipes and deriving table names
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file (or process substitution)
    /// * `format` - File format
    /// * `modifiers` - Optional modifiers (for CSV/TSV)
    /// * `pipe_counter` - Mutable counter for auto-numbering pipes
    ///
    /// # Table Naming Priority
    ///
    /// 1. Explicit name from modifiers (`name=tablename`)
    /// 2. Auto-numbered for pipes (`_1`, `_2`, `_3`, ...)
    /// 3. Derived from filename (extension stripped)
    pub fn new(
        path: PathBuf,
        format: FileFormat,
        modifiers: Option<CsvModifiers>,
        pipe_counter: &mut usize,
    ) -> Self {
        let is_pipe = Self::is_process_substitution(&path);

        // Determine table name
        let table_name = if let Some(ref mods) = modifiers {
            if let Some(ref name) = mods.name {
                // Explicit name from modifiers
                name.clone()
            } else if is_pipe {
                // Auto-number pipes
                let name = format!("_{}", pipe_counter);
                *pipe_counter += 1;
                name
            } else {
                // Derive from filename
                Self::derive_table_name(&path)
            }
        } else if is_pipe {
            // Auto-number pipes
            let name = format!("_{}", pipe_counter);
            *pipe_counter += 1;
            name
        } else {
            // Derive from filename
            Self::derive_table_name(&path)
        };

        FileInput {
            path,
            table_name,
            format,
            modifiers,
            is_pipe,
        }
    }

    /// Check if path is a process substitution (named pipe)
    ///
    /// Detects common patterns for process substitution on different platforms:
    /// - Unix/Linux: `/dev/fd/*` or `/proc/self/fd/*`
    /// - macOS: `/dev/fd/*`
    fn is_process_substitution(path: &Path) -> bool {
        let path_str = path.to_string_lossy();

        // Unix: /dev/fd/*
        if path_str.starts_with("/dev/fd/") {
            return true;
        }

        // MacOS: /dev/fd/* or sometimes other patterns
        if path_str.starts_with("/dev/") && path_str.contains("/fd/") {
            return true;
        }

        // Linux: can also be /proc/self/fd/*
        if path_str.starts_with("/proc/self/fd/") {
            return true;
        }

        false
    }

    /// Derive table name from filename (strip extension)
    fn derive_table_name(path: &Path) -> String {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string()
    }

    /// Get fully qualified table name with namespace
    pub fn qualified_name(&self) -> String {
        format!("tools.{}", self.table_name)
    }

    /// Get display string for discovery mode
    pub fn display_string(&self) -> String {
        let source = if self.is_pipe {
            "<process substitution>".to_string()
        } else {
            self.path.display().to_string()
        };

        format!("{:20} (from {})", self.qualified_name(), source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regular_file() {
        let mut counter = 1;
        let input = FileInput::new(
            PathBuf::from("users.csv"),
            FileFormat::Csv,
            None,
            &mut counter,
        );

        assert_eq!(input.table_name, "users");
        assert!(!input.is_pipe);
        assert_eq!(input.qualified_name(), "tools.users");
        assert_eq!(counter, 1); // Not incremented for regular files
    }

    #[test]
    fn test_regular_file_with_path() {
        let mut counter = 1;
        let input = FileInput::new(
            PathBuf::from("/tmp/data/orders.json"),
            FileFormat::Json,
            None,
            &mut counter,
        );

        assert_eq!(input.table_name, "orders");
        assert!(!input.is_pipe);
        assert_eq!(input.qualified_name(), "tools.orders");
    }

    #[test]
    fn test_pipe_auto_number() {
        let mut counter = 1;
        let input = FileInput::new(
            PathBuf::from("/dev/fd/63"),
            FileFormat::Csv,
            None,
            &mut counter,
        );

        assert_eq!(input.table_name, "_1");
        assert!(input.is_pipe);
        assert_eq!(input.qualified_name(), "tools._1");
        assert_eq!(counter, 2); // Incremented after auto-numbering
    }

    #[test]
    fn test_multiple_pipes_auto_number() {
        let mut counter = 1;

        let input1 = FileInput::new(
            PathBuf::from("/dev/fd/63"),
            FileFormat::Csv,
            None,
            &mut counter,
        );
        assert_eq!(input1.table_name, "_1");
        assert_eq!(counter, 2);

        let input2 = FileInput::new(
            PathBuf::from("/dev/fd/64"),
            FileFormat::Csv,
            None,
            &mut counter,
        );
        assert_eq!(input2.table_name, "_2");
        assert_eq!(counter, 3);
    }

    #[test]
    fn test_proc_self_fd_detection() {
        let mut counter = 1;
        let input = FileInput::new(
            PathBuf::from("/proc/self/fd/5"),
            FileFormat::Csv,
            None,
            &mut counter,
        );

        assert!(input.is_pipe);
        assert_eq!(input.table_name, "_1");
    }

    #[test]
    fn test_explicit_name() {
        let mut counter = 1;
        let mods = CsvModifiers {
            name: Some("mydata".to_string()),
            ..Default::default()
        };

        let input = FileInput::new(
            PathBuf::from("/dev/fd/63"),
            FileFormat::Csv,
            Some(mods),
            &mut counter,
        );

        assert_eq!(input.table_name, "mydata");
        assert!(input.is_pipe);
        assert_eq!(input.qualified_name(), "tools.mydata");
        assert_eq!(counter, 1); // Counter not incremented when explicit name
    }

    #[test]
    fn test_explicit_name_overrides_filename() {
        let mut counter = 1;
        let mods = CsvModifiers {
            name: Some("renamed".to_string()),
            ..Default::default()
        };

        let input = FileInput::new(
            PathBuf::from("users.csv"),
            FileFormat::Csv,
            Some(mods),
            &mut counter,
        );

        assert_eq!(input.table_name, "renamed");
        assert!(!input.is_pipe);
        assert_eq!(input.qualified_name(), "tools.renamed");
    }

    #[test]
    fn test_display_string_regular_file() {
        let mut counter = 1;
        let input = FileInput::new(
            PathBuf::from("users.csv"),
            FileFormat::Csv,
            None,
            &mut counter,
        );

        let display = input.display_string();
        assert!(display.contains("tools.users"));
        assert!(display.contains("users.csv"));
    }

    #[test]
    fn test_display_string_pipe() {
        let mut counter = 1;
        let input = FileInput::new(
            PathBuf::from("/dev/fd/63"),
            FileFormat::Csv,
            None,
            &mut counter,
        );

        let display = input.display_string();
        assert!(display.contains("tools._1"));
        assert!(display.contains("<process substitution>"));
    }

    #[test]
    fn test_derive_table_name_no_extension() {
        let mut counter = 1;
        let input = FileInput::new(
            PathBuf::from("datafile"),
            FileFormat::Csv,
            None,
            &mut counter,
        );

        assert_eq!(input.table_name, "datafile");
    }

    #[test]
    fn test_derive_table_name_multiple_dots() {
        let mut counter = 1;
        let input = FileInput::new(
            PathBuf::from("my.data.file.csv"),
            FileFormat::Csv,
            None,
            &mut counter,
        );

        assert_eq!(input.table_name, "my.data.file");
    }

    #[test]
    fn test_format_variants() {
        let mut counter = 1;

        let json = FileInput::new(
            PathBuf::from("data.json"),
            FileFormat::Json,
            None,
            &mut counter,
        );
        assert!(matches!(json.format, FileFormat::Json));

        let csv = FileInput::new(
            PathBuf::from("data.csv"),
            FileFormat::Csv,
            None,
            &mut counter,
        );
        assert!(matches!(csv.format, FileFormat::Csv));

        let tsv = FileInput::new(
            PathBuf::from("data.tsv"),
            FileFormat::Tsv,
            None,
            &mut counter,
        );
        assert!(matches!(tsv.format, FileFormat::Tsv));

        let jsonl = FileInput::new(
            PathBuf::from("data.jsonl"),
            FileFormat::JsonL,
            None,
            &mut counter,
        );
        assert!(matches!(jsonl.format, FileFormat::JsonL));
    }
}
