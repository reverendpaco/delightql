#[derive(Debug)]
#[allow(dead_code)]
pub enum GeneratorError {
    Error(String),
}

impl std::fmt::Display for GeneratorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeneratorError::Error(msg) => write!(f, "Generator error: {}", msg),
        }
    }
}

impl std::error::Error for GeneratorError {}
