fn main() {
    #[cfg(feature = "bundled-parser")]
    {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = manifest_dir
            .parent()
            .expect("Could not find crates directory")
            .parent()
            .expect("Could not find workspace root");
        let grammar_src = workspace_root.join("grammar_dql").join("src");

        println!(
            "cargo:rerun-if-changed={}",
            grammar_src.join("parser.c").display()
        );

        cc::Build::new()
            .include(&grammar_src)
            .file(grammar_src.join("parser.c"))
            .compile("tree-sitter-delightql-v2-fmt");
    }
}
