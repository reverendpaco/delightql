//! Theme configuration for syntax highlighting
//!
//! Supports both named colors (red, blue, etc.) and hex colors (#rrggbb)

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ThemeConfig {
    #[serde(default)]
    pub colors: HashMap<String, String>,
}

impl ThemeConfig {
    /// Load theme from a TOML file
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read theme file: {}", path.display()))?;

        let config: ThemeConfig = toml::from_str(&content)
            .with_context(|| format!("Failed to parse theme file: {}", path.display()))?;

        Ok(config)
    }

    /// Get the ANSI color code for a given capture name
    pub fn get_color(&self, capture_name: &str) -> Option<String> {
        self.colors
            .get(capture_name)
            .map(|color_spec| color_name_to_ansi(color_spec))
    }

    /// Get all capture names defined in the theme
    #[allow(dead_code)]
    pub fn capture_names(&self) -> Vec<String> {
        self.colors.keys().cloned().collect()
    }
}

/// Convert a color name or hex code to ANSI escape sequence
fn color_name_to_ansi(color_spec: &str) -> String {
    // Handle bold prefix
    let (is_bold, color_spec) = if let Some(stripped) = color_spec.strip_prefix("bold-") {
        (true, stripped)
    } else {
        (false, color_spec)
    };

    // Convert color to ANSI code
    let color_code = if color_spec.starts_with('#') {
        // Hex color: #rrggbb
        hex_to_ansi_24bit(color_spec)
    } else {
        // Named color
        named_color_to_ansi(color_spec)
    };

    // Add bold if needed
    if is_bold {
        format!("\x1b[1;{}", color_code)
    } else {
        format!("\x1b[{}", color_code)
    }
}

/// Convert hex color to 24-bit ANSI escape code
fn hex_to_ansi_24bit(hex: &str) -> String {
    let hex = hex.trim_start_matches('#');

    if hex.len() != 6 {
        // Fallback to white if invalid
        return "37m".to_string();
    }

    let r = u8::from_str_radix(&hex[0..2], 16).unwrap_or(255);
    let g = u8::from_str_radix(&hex[2..4], 16).unwrap_or(255);
    let b = u8::from_str_radix(&hex[4..6], 16).unwrap_or(255);

    format!("38;2;{};{};{}m", r, g, b)
}

/// Convert named color to ANSI code
fn named_color_to_ansi(name: &str) -> String {
    match name.to_lowercase().as_str() {
        "black" => "30m",
        "red" => "31m",
        "green" => "32m",
        "yellow" => "33m",
        "blue" => "34m",
        "magenta" | "purple" => "35m",
        "cyan" => "36m",
        "white" => "37m",
        "gray" | "grey" => "90m",
        "bright-red" => "91m",
        "bright-green" => "92m",
        "bright-yellow" => "93m",
        "bright-blue" => "94m",
        "bright-magenta" => "95m",
        "bright-cyan" => "96m",
        "bright-white" => "97m",
        _ => "37m", // Default to white
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_named_colors() {
        assert_eq!(color_name_to_ansi("red"), "\x1b[31m");
        assert_eq!(color_name_to_ansi("blue"), "\x1b[34m");
        assert_eq!(color_name_to_ansi("bold-green"), "\x1b[1;32m");
    }

    #[test]
    fn test_hex_colors() {
        assert_eq!(color_name_to_ansi("#ff0000"), "\x1b[38;2;255;0;0m");
        assert_eq!(color_name_to_ansi("#00ff00"), "\x1b[38;2;0;255;0m");
        assert_eq!(color_name_to_ansi("bold-#0000ff"), "\x1b[1;38;2;0;0;255m");
    }
}
