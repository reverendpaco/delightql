use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .expect("Could not find crates directory")
        .parent()
        .expect("Could not find workspace root");

    let grammar_src = workspace_root.join("grammar_dql/src");
    let parser_c = grammar_src.join("parser.c");

    println!("cargo:rerun-if-changed={}", parser_c.display());
    println!(
        "cargo:rerun-if-changed={}",
        grammar_src
            .join("tree_sitter")
            .join("parser.h")
            .display()
    );

    cc::Build::new()
        .include(&grammar_src)
        .file(&parser_c)
        .compile("tree-sitter-delightql-v2");
}
