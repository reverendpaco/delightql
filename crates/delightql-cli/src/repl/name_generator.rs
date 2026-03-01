/// Deterministic name generator for REPL auto-captured queries.
///
/// Produces names in `adjective_color_animal` format (e.g. "fuzzy_red_wombat").
/// Counter-based selection ensures no collisions within a session.
/// 50 adjectives * 4 colors * 50 animals = 10,000 unique names.

const ADJECTIVES: &[&str] = &[
    "fuzzy", "calm", "bright", "swift", "bold",
    "keen", "warm", "cool", "soft", "sharp",
    "quick", "slow", "tall", "wide", "deep",
    "light", "dark", "thin", "flat", "round",
    "firm", "mild", "pure", "raw", "shy",
    "odd", "dry", "wet", "old", "new",
    "kind", "wild", "tame", "fair", "grim",
    "loud", "hush", "vast", "snug", "trim",
    "glad", "free", "rare", "rich", "lean",
    "pale", "dull", "neat", "long", "tiny",
];

const COLORS: &[&str] = &["red", "blue", "green", "amber"];

const ANIMALS: &[&str] = &[
    "wombat", "parrot", "otter", "falcon", "bison",
    "crane", "finch", "gecko", "heron", "koala",
    "lemur", "moose", "newt", "osprey", "panda",
    "quail", "raven", "sloth", "trout", "viper",
    "whale", "yak", "zebra", "badger", "cobra",
    "dingo", "egret", "fox", "grouse", "hippo",
    "ibis", "jackal", "kiwi", "llama", "marten",
    "narwhal", "ocelot", "puffin", "robin", "seal",
    "tern", "urchin", "vole", "wren", "condor",
    "dove", "elk", "frog", "gull", "hawk",
];

pub struct ReplNameGenerator {
    counter: u32,
}

impl ReplNameGenerator {
    pub fn new() -> Self {
        Self { counter: 0 }
    }

    pub fn generate(&mut self) -> String {
        let n = self.counter as usize;
        let adj = ADJECTIVES[n % ADJECTIVES.len()];
        let color = COLORS[(n / ADJECTIVES.len()) % COLORS.len()];
        let animal = ANIMALS[(n / (ADJECTIVES.len() * COLORS.len())) % ANIMALS.len()];
        self.counter += 1;
        format!("{}_{}_{}", adj, color, animal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_unique_names() {
        let mut gen = ReplNameGenerator::new();
        let mut names = std::collections::HashSet::new();
        for _ in 0..200 {
            let name = gen.generate();
            assert!(names.insert(name.clone()), "duplicate name: {}", name);
        }
    }

    #[test]
    fn first_name_is_fuzzy_red_wombat() {
        let mut gen = ReplNameGenerator::new();
        assert_eq!(gen.generate(), "fuzzy_red_wombat");
    }

    #[test]
    fn names_are_valid_identifiers() {
        let mut gen = ReplNameGenerator::new();
        for _ in 0..100 {
            let name = gen.generate();
            assert!(name.chars().all(|c| c.is_ascii_lowercase() || c == '_'));
        }
    }
}
