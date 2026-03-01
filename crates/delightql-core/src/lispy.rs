pub trait ToLispy {
    fn to_lispy(&self) -> String;
}

// Implementations for standard Rust types

impl ToLispy for String {
    fn to_lispy(&self) -> String {
        format!("\"{}\"", self)
    }
}

impl ToLispy for &str {
    fn to_lispy(&self) -> String {
        format!("\"{}\"", self)
    }
}

impl ToLispy for delightql_types::SqlIdentifier {
    fn to_lispy(&self) -> String {
        format!("\"{}\"", self.as_str())
    }
}

impl ToLispy for bool {
    fn to_lispy(&self) -> String {
        if *self { "true".to_string() } else { "false".to_string() }
    }
}

impl ToLispy for i64 {
    fn to_lispy(&self) -> String {
        self.to_string()
    }
}

impl ToLispy for usize {
    fn to_lispy(&self) -> String {
        self.to_string()
    }
}

impl<T: ToLispy> ToLispy for Option<T> {
    fn to_lispy(&self) -> String {
        match self {
            Some(value) => value.to_lispy(),
            None => "nil".to_string(),
        }
    }
}

impl<T: ToLispy> ToLispy for Vec<T> {
    fn to_lispy(&self) -> String {
        let items = self.iter()
            .map(|item| item.to_lispy())
            .collect::<Vec<_>>()
            .join(" ");
        format!("[{}]", items)
    }
}

impl<T: ToLispy> ToLispy for Box<T> {
    fn to_lispy(&self) -> String {
        self.as_ref().to_lispy()
    }
}

impl<T1: ToLispy, T2: ToLispy> ToLispy for (T1, T2) {
    fn to_lispy(&self) -> String {
        format!("({} . {})", self.0.to_lispy(), self.1.to_lispy())
    }
}

impl<T1: ToLispy, T2: ToLispy, T3: ToLispy> ToLispy for (T1, T2, T3) {
    fn to_lispy(&self) -> String {
        format!("({} {} {})", self.0.to_lispy(), self.1.to_lispy(), self.2.to_lispy())
    }
}