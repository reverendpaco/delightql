use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
/// Tab completion support for REPL
///
/// This module contains the rustyline helper implementations for tab completion
/// of dot commands and column names in the REPL.
use rustyline::{Context as RustylineContext, Helper};
// Temporarily disabled during pipeline migration
// use delightql_core::schema::Schema;

/// Context for determining what type of completion to provide
#[derive(Debug, Clone)]
enum CompletionContext {
    /// Line starts with '.', complete dot commands
    DotCommand,
    /// Complete column names from the given tables
    ColumnName { tables: Vec<String> },
    /// Unknown context, no completions
    Unknown,
}

/// Completer for dot commands and column names in the REPL
#[derive(Clone)]
pub struct DotCommandCompleter {
    commands: Vec<String>,
    // schema: Option<Arc<dyn Schema>>, // Temporarily disabled
}

impl DotCommandCompleter {
    pub fn new() -> Self {
        Self {
            commands: vec![
                ".help".to_string(),
                ".exit".to_string(),
                ".info".to_string(),
                ".debug-last".to_string(),
                ".debug-last-dump".to_string(),
                ".version".to_string(),
                ".format".to_string(),
                ".to".to_string(),
                ".dql".to_string(),
                ".sql".to_string(),
                ".bug".to_string(),
                ".multiline".to_string(),
            ],
            // schema: None, // Temporarily disabled
        }
    }

    /// Detect the context for completion
    fn detect_context(&self, line: &str, pos: usize) -> CompletionContext {
        let text = &line[..pos];

        // Check for dot command
        if text.starts_with('.') {
            return CompletionContext::DotCommand;
        }

        // Check for column name context
        // Pattern 1: "table(*), " - filter condition
        // Pattern 2: "table(col, " - field list
        // Pattern 3: "table(*) |> [" - projection
        // Pattern 4: Multiple tables

        // Simple heuristic: if we have a table name followed by parentheses or pipe
        if let Some(tables) = self.extract_tables_in_scope(text) {
            if !tables.is_empty() {
                return CompletionContext::ColumnName { tables };
            }
        }

        CompletionContext::Unknown
    }

    /// Extract table names from the query that are in scope
    fn extract_tables_in_scope(&self, text: &str) -> Option<Vec<String>> {
        let mut tables = Vec::new();

        // Look for DelightQL patterns:
        // 1. tablename(*) or tablename(columns...)
        // 2. Multiple tables: table1(*), table2(*)
        // 3. After pipe operator, maintain context

        // Handle pipe operator - everything before last pipe is context
        let context_text = if let Some(pipe_pos) = text.rfind("|>") {
            // Get the part before the pipe for table context

            // But work with the full text for finding current position
            &text[..pipe_pos]
        } else {
            text
        };

        // Look for table patterns in the context
        // Match patterns like: word(*) or word(anything)
        // Simple pattern matching without regex (to avoid adding dependency)
        let mut i = 0;
        let bytes = context_text.as_bytes();
        while i < bytes.len() {
            // Look for identifier start
            if (bytes[i] as char).is_alphabetic() || bytes[i] == b'_' {
                let start = i;
                // Scan identifier
                while i < bytes.len() && ((bytes[i] as char).is_alphanumeric() || bytes[i] == b'_')
                {
                    i += 1;
                }
                let table_name = &context_text[start..i];

                // Skip whitespace
                while i < bytes.len() && (bytes[i] as char).is_whitespace() {
                    i += 1;
                }

                // Check for '('
                if i < bytes.len() && bytes[i] == b'(' {
                    // Found a table reference
                    tables.push(table_name.to_string());
                }
            } else {
                i += 1;
            }
        }

        // Remove duplicates while preserving order
        let mut unique_tables = Vec::new();
        for table in tables {
            if !unique_tables.contains(&table) {
                unique_tables.push(table);
            }
        }

        if unique_tables.is_empty() {
            None
        } else {
            Some(unique_tables)
        }
    }

    /// Get column completions for the given tables
    fn get_column_completions(&self, _tables: &[String], _prefix: &str) -> Vec<Pair> {
        // Temporarily disabled during migration - schema support removed
        Vec::new()
    }
}

impl Completer for DotCommandCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &RustylineContext<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let context = self.detect_context(line, pos);

        match context {
            CompletionContext::DotCommand => {
                // Complete dot commands
                let text = &line[..pos];
                let mut matches = Vec::new();
                for command in &self.commands {
                    if command.starts_with(text) {
                        matches.push(Pair {
                            display: command.clone(),
                            replacement: command.clone(),
                        });
                    }
                }
                Ok((0, matches))
            }
            CompletionContext::ColumnName { tables } => {
                // Find the start of the current word being typed
                let text = &line[..pos];
                let word_start = text
                    .rfind(|c: char| {
                        c.is_whitespace() || c == ',' || c == '(' || c == '[' || c == '>'
                    })
                    .map(|i| i + 1)
                    .unwrap_or(0);

                let prefix = &text[word_start..];
                let completions = self.get_column_completions(&tables, prefix);

                Ok((word_start, completions))
            }
            CompletionContext::Unknown => {
                // No completions available
                Ok((pos, Vec::new()))
            }
        }
    }
}

impl Hinter for DotCommandCompleter {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &RustylineContext<'_>) -> Option<String> {
        None
    }
}

impl Highlighter for DotCommandCompleter {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> std::borrow::Cow<'b, str> {
        std::borrow::Cow::Borrowed(prompt)
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> std::borrow::Cow<'h, str> {
        std::borrow::Cow::Borrowed(hint)
    }

    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> std::borrow::Cow<'l, str> {
        #[cfg(feature = "prettify")]
        {
            // Use our syntax highlighter for DelightQL syntax
            crate::repl::syntax_highlighter::highlight_line(line)
        }
        #[cfg(not(feature = "prettify"))]
        {
            // No syntax highlighting when prettify feature is disabled
            std::borrow::Cow::Borrowed(line)
        }
    }

    fn highlight_char(&self, _line: &str, _pos: usize) -> bool {
        // Always refresh to ensure syntax highlighting is up to date
        true
    }

    fn highlight_candidate<'c>(
        &self,
        candidate: &'c str,
        _completion: rustyline::CompletionType,
    ) -> std::borrow::Cow<'c, str> {
        std::borrow::Cow::Borrowed(candidate)
    }
}

impl Validator for DotCommandCompleter {
    fn validate(&self, _ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        Ok(ValidationResult::Valid(None))
    }

    fn validate_while_typing(&self) -> bool {
        false
    }
}

impl Helper for DotCommandCompleter {}

/* Temporarily disabled during migration
#[cfg(test)]
mod tests {
    use super::*;
    use delightql_core::schema::{MockSchema, TableInfo, ColumnInfo};

    fn create_test_schema() -> Arc<MockSchema> {
        let mut schema = MockSchema::new();

        // Add users table
        schema.add_table(TableInfo {
            name: "users".to_string(),
            // schema: None, // Temporarily disabled
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                },
                ColumnInfo {
                    name: "email".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                },
                ColumnInfo {
                    name: "age".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                },
            ],
        });

        // Add orders table
        schema.add_table(TableInfo {
            name: "orders".to_string(),
            // schema: None, // Temporarily disabled
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                },
                ColumnInfo {
                    name: "user_id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                },
                ColumnInfo {
                    name: "total".to_string(),
                    data_type: "DECIMAL".to_string(),
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                },
            ],
        });

        Arc::new(schema)
    }

    #[test]
    fn test_detect_dot_command_context() {
        let completer = DotCommandCompleter::new();
        let context = completer.detect_context(".hel", 4);
        match context {
            CompletionContext::DotCommand => {},
            _ => panic!("Expected DotCommand context"),
        }
    }

    #[test]
    fn test_detect_column_context_after_table() {
        let completer = DotCommandCompleter::new();
        let context = completer.detect_context("users(*), ", 10);
        match context {
            CompletionContext::ColumnName { tables } => {
                assert_eq!(tables, vec!["users".to_string()]);
            },
            _ => panic!("Expected ColumnName context"),
        }
    }

    #[test]
    fn test_detect_column_context_in_field_list() {
        let completer = DotCommandCompleter::new();
        let context = completer.detect_context("users(name, ", 12);
        match context {
            CompletionContext::ColumnName { tables } => {
                assert_eq!(tables, vec!["users".to_string()]);
            },
            _ => panic!("Expected ColumnName context"),
        }
    }

    #[test]
    fn test_detect_column_context_after_pipe() {
        let completer = DotCommandCompleter::new();
        let context = completer.detect_context("users(*) |> [", 13);
        match context {
            CompletionContext::ColumnName { tables } => {
                assert_eq!(tables, vec!["users".to_string()]);
            },
            _ => panic!("Expected ColumnName context"),
        }
    }

    #[test]
    fn test_detect_multiple_tables_context() {
        let completer = DotCommandCompleter::new();
        let context = completer.detect_context("orders(*), users(*), ", 21);
        match context {
            CompletionContext::ColumnName { tables } => {
                assert_eq!(tables.len(), 2);
                assert!(tables.contains(&"orders".to_string()));
                assert!(tables.contains(&"users".to_string()));
            },
            _ => panic!("Expected ColumnName context with multiple tables"),
        }
    }

    #[test]
    fn test_column_completions_single_table() {
        let schema = create_test_schema();
        let completer = DotCommandCompleter::with_schema(schema);

        let completions = completer.get_column_completions(&["users".to_string()], "");
        assert_eq!(completions.len(), 4);

        let names: Vec<String> = completions.iter().map(|p| p.display.clone()).collect();
        assert!(names.contains(&"id".to_string()));
        assert!(names.contains(&"name".to_string()));
        assert!(names.contains(&"email".to_string()));
        assert!(names.contains(&"age".to_string()));
    }

    #[test]
    fn test_column_completions_with_prefix() {
        let schema = create_test_schema();
        let completer = DotCommandCompleter::with_schema(schema);

        let completions = completer.get_column_completions(&["users".to_string()], "em");
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].display, "email");
    }

    #[test]
    fn test_column_completions_multiple_tables() {
        let schema = create_test_schema();
        let completer = DotCommandCompleter::with_schema(schema);

        let completions = completer.get_column_completions(
            &["users".to_string(), "orders".to_string()],
            ""
        );

        // Should have qualified names
        let names: Vec<String> = completions.iter().map(|p| p.display.clone()).collect();
        assert!(names.contains(&"users.id".to_string()));
        assert!(names.contains(&"users.name".to_string()));
        assert!(names.contains(&"orders.id".to_string()));
        assert!(names.contains(&"orders.total".to_string()));
    }

    #[test]
    fn test_complete_dot_command() {
        let completer = DotCommandCompleter::new();
        // For testing, we don't need real history, just need to satisfy the API
        // Create a mock context without history

        // We can't easily create a Context in tests, so test the methods directly
        // Test context detection
        let context = completer.detect_context(".hel", 4);
        assert!(matches!(context, CompletionContext::DotCommand));

        // Test completion generation for dot commands
        let mut result = Vec::new();
        for command in &completer.commands {
            if command.starts_with(".hel") {
                result.push(command.clone());
            }
        }
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ".help");
    }

    #[test]
    fn test_complete_columns_after_table() {
        let schema = create_test_schema();
        let completer = DotCommandCompleter::with_schema(schema);

        // Test context detection
        let context = completer.detect_context("users(*), em", 12);
        assert!(matches!(context, CompletionContext::ColumnName { .. }));

        // Test column completion
        let completions = completer.get_column_completions(&["users".to_string()], "em");
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].replacement, "email");
    }
}
*/
// End of temporarily disabled test module
