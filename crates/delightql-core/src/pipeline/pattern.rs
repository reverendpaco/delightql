//! Pattern matching utilities for column selection
//!
//! Provides POSIX BRE-compatible pattern matching for column selection,
//! converting simple BRE patterns to Rust regex patterns.

use crate::error::{DelightQLError, Result};

/// Convert a POSIX BRE-style pattern to Rust regex pattern
///
/// This implements a minimal subset of BRE for column matching:
/// - Basic character matching
/// - . (any character)
/// - * (zero or more of preceding)
/// - ^ (anchor to start) - only when explicitly provided
/// - $ (anchor to end) - only when explicitly provided
/// - [] character classes
/// - Escaping with \
///
/// Like grep, patterns match substrings by default (no implicit anchors).
/// Use ^ and $ explicitly when you want anchored matches.
pub fn bre_to_rust_regex(pattern: &str) -> Result<String> {
    let mut result = String::with_capacity(pattern.len() * 2);
    let mut chars = pattern.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            // Anchors - these are the same in BRE and Rust regex
            '^' | '$' => result.push(c),

            // Dot - any character (same in both)
            '.' => result.push('.'),

            // Star - zero or more (same in both)
            '*' => result.push('*'),

            // Character class start
            '[' => {
                result.push('[');
                // Consume everything until closing ]
                let mut found_close = false;
                for ch in chars.by_ref() {
                    result.push(ch);
                    if ch == ']' {
                        found_close = true;
                        break;
                    }
                }
                if !found_close {
                    return Err(DelightQLError::parse_error(
                        "Unclosed character class in pattern",
                    ));
                }
            }

            // Escape sequences
            '\\' => {
                if let Some(next) = chars.next() {
                    match next {
                        // BRE metacharacters that need escaping
                        '.' | '*' | '[' | ']' | '^' | '$' | '\\' => {
                            result.push('\\');
                            result.push(next);
                        }
                        // In BRE, \+ and \? are the extended versions
                        '+' => result.push('+'), // In Rust regex, + is already special
                        '?' => result.push('?'), // In Rust regex, ? is already special

                        // Everything else: pass through as literal
                        c => {
                            // In BRE, unknown escapes are treated as literals
                            result.push('\\');
                            result.push(c);
                        }
                    }
                } else {
                    // Trailing backslash
                    return Err(DelightQLError::parse_error("Trailing backslash in pattern"));
                }
            }

            // Characters that are special in Rust regex but not in BRE
            // These need to be escaped for Rust regex
            '+' | '?' | '(' | ')' | '{' | '}' | '|' => {
                result.push('\\');
                result.push(c);
            }

            // All other characters are literals
            _ => result.push(c),
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_patterns() {
        // Simple literals
        assert_eq!(bre_to_rust_regex("abc").unwrap(), "abc");
        assert_eq!(bre_to_rust_regex("user_id").unwrap(), "user_id");

        // Anchors
        assert_eq!(bre_to_rust_regex("^id").unwrap(), "^id");
        assert_eq!(bre_to_rust_regex("name$").unwrap(), "name$");
        assert_eq!(bre_to_rust_regex("^user$").unwrap(), "^user$");

        // Wildcards
        assert_eq!(bre_to_rust_regex(".*").unwrap(), ".*");
        assert_eq!(bre_to_rust_regex("user.*").unwrap(), "user.*");
        assert_eq!(bre_to_rust_regex(".*_id").unwrap(), ".*_id");

        // Character classes
        assert_eq!(bre_to_rust_regex("[abc]").unwrap(), "[abc]");
        assert_eq!(bre_to_rust_regex("[a-z]").unwrap(), "[a-z]");
        assert_eq!(bre_to_rust_regex("[^0-9]").unwrap(), "[^0-9]");
    }

    #[test]
    fn test_escaping() {
        // Escape BRE metacharacters
        assert_eq!(bre_to_rust_regex(r"\.txt").unwrap(), r"\.txt");
        assert_eq!(bre_to_rust_regex(r"\*").unwrap(), r"\*");
        assert_eq!(bre_to_rust_regex(r"\[abc\]").unwrap(), r"\[abc\]");

        // BRE extended operators (become normal in Rust regex)
        assert_eq!(bre_to_rust_regex(r"\+").unwrap(), "+");
        assert_eq!(bre_to_rust_regex(r"\?").unwrap(), "?");
    }

    #[test]
    fn test_rust_special_chars() {
        // These are special in Rust but not BRE, so need escaping
        assert_eq!(bre_to_rust_regex("a+b").unwrap(), r"a\+b");
        assert_eq!(bre_to_rust_regex("a?b").unwrap(), r"a\?b");
        assert_eq!(bre_to_rust_regex("(abc)").unwrap(), r"\(abc\)");
        assert_eq!(bre_to_rust_regex("a|b").unwrap(), r"a\|b");
        assert_eq!(bre_to_rust_regex("{1,2}").unwrap(), r"\{1,2\}");
    }

    #[test]
    fn test_column_patterns() {
        // Common column selection patterns
        assert_eq!(bre_to_rust_regex(".*_name").unwrap(), ".*_name");
        assert_eq!(bre_to_rust_regex("^user_.*").unwrap(), "^user_.*");
        assert_eq!(bre_to_rust_regex(".*_id$").unwrap(), ".*_id$");
        assert_eq!(bre_to_rust_regex("[a-z]*_[0-9]*").unwrap(), "[a-z]*_[0-9]*");
    }

    #[test]
    fn test_error_cases() {
        // Unclosed character class
        assert!(bre_to_rust_regex("[abc").is_err());

        // Trailing backslash
        assert!(bre_to_rust_regex("abc\\").is_err());
    }
}
