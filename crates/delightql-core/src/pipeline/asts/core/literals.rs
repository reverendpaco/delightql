use super::metadata::NamespacePath;
use crate::{lispy::ToLispy, ToLispy};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub enum LiteralValue {
    String(String),
    Number(String),
    Boolean(bool),
    Null,
}

impl std::fmt::Display for LiteralValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LiteralValue::String(s) => write!(f, "{}", s),
            LiteralValue::Number(n) => write!(f, "{}", n),
            LiteralValue::Boolean(b) => write!(f, "{}", b),
            LiteralValue::Null => write!(f, "null"),
        }
    }
}

/// Column ordinal reference: |N| or table|N|
///
/// Like Lvar: namespace_path (WHERE) + qualifier (WHICH table) + position
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnOrdinal {
    pub position: u16,
    pub reverse: bool,
    /// Table qualifier/reference
    pub qualifier: Option<String>,
    /// Namespace path
    pub namespace_path: NamespacePath,
    pub alias: Option<String>,
    /// Whether this is a glob ordinal (|*|) representing all columns by position
    #[serde(default)]
    pub glob: bool,
}

impl ToLispy for ColumnOrdinal {
    fn to_lispy(&self) -> String {
        if self.glob {
            let qual_str = self
                .qualifier
                .as_ref()
                .map(|q| format!("{}|", q))
                .unwrap_or_default();
            return format!("|{}*|", qual_str);
        }

        let pos_str = if self.reverse {
            format!("-{}", self.position)
        } else {
            self.position.to_string()
        };

        let qual_str = self
            .qualifier
            .as_ref()
            .map(|q| format!("{}|", q))
            .unwrap_or_default();

        format!("|{}{}|", qual_str, pos_str)
    }
}

/// Column range reference: |N:M| or table|N:M|
///
/// Like Lvar: namespace_path (WHERE) + qualifier (WHICH table) + range
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnRange {
    pub start: Option<(u16, bool)>,
    pub end: Option<(u16, bool)>,
    /// Table qualifier/reference
    pub qualifier: Option<String>,
    /// Namespace path
    pub namespace_path: NamespacePath,
}

impl ToLispy for ColumnRange {
    fn to_lispy(&self) -> String {
        let format_pos = |(pos, rev): (u16, bool)| {
            if rev {
                format!("-{}", pos)
            } else {
                pos.to_string()
            }
        };

        let start_str = self.start.map(format_pos).unwrap_or_default();
        let end_str = self.end.map(format_pos).unwrap_or_default();

        let qual_str = self
            .qualifier
            .as_ref()
            .map(|q| format!("{}|", q))
            .unwrap_or_default();

        format!("|{}{}:{}|", qual_str, start_str, end_str)
    }
}
