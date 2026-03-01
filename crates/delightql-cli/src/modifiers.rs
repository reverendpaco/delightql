//! CSV/TSV modifier parsing
//!
//! Parses modifier strings like "header,!trim,name=mydata" into structured configuration.
//!
//! # Syntax
//!
//! - `feature` or `+feature` - Enable boolean feature (e.g., `header`, `trim`)
//! - `!feature` - Disable boolean feature (e.g., `!quotes`, `!trim`)
//! - `name=value` - Set parameter (e.g., `name=tablename`)
//!
//! # Examples
//!
//! ```
//! use delightql_cli::modifiers::CsvModifiers;
//!
//! let mods = CsvModifiers::parse("header,name=mydata").unwrap();
//! assert!(mods.header);
//! assert_eq!(mods.name.as_deref(), Some("mydata"));
//! ```

/// Parsed modifiers for CSV/TSV files
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CsvModifiers {
    /// Use first row as column names
    pub header: bool,

    /// Trim whitespace (future)
    pub trim: Option<bool>,

    /// Handle quoted fields (future)
    pub quotes: Option<bool>,

    /// Override table name
    pub name: Option<String>,
}

impl CsvModifiers {
    /// Parse modifier string like "header,!trim,name=mydata"
    ///
    /// # Syntax
    ///
    /// - Comma-separated list of modifiers
    /// - `feature` or `+feature` enables a boolean feature
    /// - `!feature` disables a boolean feature
    /// - `key=value` sets a parameter
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Unknown modifier is specified
    /// - Unknown parameter key is used
    /// - Invalid syntax is encountered
    pub fn parse(s: &str) -> Result<Self, String> {
        let mut mods = CsvModifiers::default();

        for part in s.split(',') {
            let part = part.trim();

            if part.is_empty() {
                continue;
            }

            // Handle name=value
            if let Some((key, value)) = part.split_once('=') {
                match key {
                    "name" => mods.name = Some(value.to_string()),
                    _ => return Err(format!("Unknown parameter: {}", key)),
                }
                continue;
            }

            // Handle +feature, !feature, or bare feature
            let (enable, feature) = if let Some(f) = part.strip_prefix('+') {
                (true, f)
            } else if let Some(f) = part.strip_prefix('!') {
                (false, f)
            } else {
                (true, part) // Bare feature name = enable
            };

            match feature {
                "header" => mods.header = enable,
                "trim" => mods.trim = Some(enable),
                "quotes" => mods.quotes = Some(enable),
                _ => return Err(format!("Unknown modifier: {}", feature)),
            }
        }

        Ok(mods)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty() {
        let mods = CsvModifiers::parse("").unwrap();
        assert_eq!(mods, CsvModifiers::default());
    }

    #[test]
    fn test_parse_header() {
        let mods = CsvModifiers::parse("header").unwrap();
        assert!(mods.header);
        assert_eq!(mods.trim, None);
        assert_eq!(mods.name, None);
    }

    #[test]
    fn test_parse_explicit_enable() {
        let mods = CsvModifiers::parse("+header").unwrap();
        assert!(mods.header);
    }

    #[test]
    fn test_parse_disable() {
        let mods = CsvModifiers::parse("!header").unwrap();
        assert!(!mods.header);
    }

    #[test]
    fn test_parse_name() {
        let mods = CsvModifiers::parse("name=mydata").unwrap();
        assert_eq!(mods.name.as_deref(), Some("mydata"));
        assert!(!mods.header); // Default is false
    }

    #[test]
    fn test_parse_combined() {
        let mods = CsvModifiers::parse("header,!trim,name=mydata").unwrap();
        assert!(mods.header);
        assert_eq!(mods.trim, Some(false));
        assert_eq!(mods.name.as_deref(), Some("mydata"));
    }

    #[test]
    fn test_parse_with_spaces() {
        let mods = CsvModifiers::parse("header, !trim, name=mydata").unwrap();
        assert!(mods.header);
        assert_eq!(mods.trim, Some(false));
        assert_eq!(mods.name.as_deref(), Some("mydata"));
    }

    #[test]
    fn test_parse_all_features() {
        let mods = CsvModifiers::parse("+header,+trim,+quotes,name=test").unwrap();
        assert!(mods.header);
        assert_eq!(mods.trim, Some(true));
        assert_eq!(mods.quotes, Some(true));
        assert_eq!(mods.name.as_deref(), Some("test"));
    }

    #[test]
    fn test_parse_disable_all() {
        let mods = CsvModifiers::parse("!header,!trim,!quotes").unwrap();
        assert!(!mods.header);
        assert_eq!(mods.trim, Some(false));
        assert_eq!(mods.quotes, Some(false));
    }

    #[test]
    fn test_parse_invalid_modifier() {
        let result = CsvModifiers::parse("invalid");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Unknown modifier: invalid");
    }

    #[test]
    fn test_parse_invalid_parameter() {
        let result = CsvModifiers::parse("foo=bar");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Unknown parameter: foo");
    }

    #[test]
    fn test_parse_name_with_underscores() {
        let mods = CsvModifiers::parse("name=my_table_name").unwrap();
        assert_eq!(mods.name.as_deref(), Some("my_table_name"));
    }

    #[test]
    fn test_parse_name_with_numbers() {
        let mods = CsvModifiers::parse("name=table123").unwrap();
        assert_eq!(mods.name.as_deref(), Some("table123"));
    }

    #[test]
    fn test_parse_trailing_comma() {
        let mods = CsvModifiers::parse("header,").unwrap();
        assert!(mods.header);
    }

    #[test]
    fn test_parse_multiple_commas() {
        let mods = CsvModifiers::parse("header,,name=test").unwrap();
        assert!(mods.header);
        assert_eq!(mods.name.as_deref(), Some("test"));
    }
}
