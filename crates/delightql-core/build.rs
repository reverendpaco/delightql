use std::path::PathBuf;
use std::process::Command;

fn main() {
    // Use CARGO_MANIFEST_DIR to find the workspace root reliably
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    // --- Embedded stdlib auto-discovery ---
    generate_stdlib_manifest(&manifest_dir);
    let workspace_root = manifest_dir
        .parent() // Go up from delightql-core to crates
        .expect("Could not find crates directory")
        .parent() // Go up from crates to workspace root
        .expect("Could not find workspace root");

    // DQL grammar (query language)
    let grammar_dql_dir = workspace_root.join("grammar_dql");
    let grammar_dql_src_dir = grammar_dql_dir.join("src");
    let grammar_dql_parser_c = grammar_dql_src_dir.join("parser.c");
    let grammar_dql_js = grammar_dql_dir.join("grammar.js");

    // Rules grammar (definition language)
    let grammar_rules_dir = workspace_root.join("grammar_rules");
    let grammar_rules_src_dir = grammar_rules_dir.join("src");
    let grammar_rules_parser_c = grammar_rules_src_dir.join("parser.c");
    let grammar_rules_js = grammar_rules_dir.join("grammar.js");

    // DDL grammar (sigil sub-language for companion tables)
    let grammar_ddl_dir = workspace_root.join("grammar_ddl");
    let grammar_ddl_src_dir = grammar_ddl_dir.join("src");
    let grammar_ddl_parser_c = grammar_ddl_src_dir.join("parser.c");
    let grammar_ddl_js = grammar_ddl_dir.join("grammar.js");

    // Tell cargo to rerun if grammar files change
    // Track both parser.c and tree_sitter/parser.h — both are needed for compilation
    println!("cargo:rerun-if-changed={}", grammar_dql_js.display());
    println!("cargo:rerun-if-changed={}", grammar_dql_parser_c.display());
    println!(
        "cargo:rerun-if-changed={}",
        grammar_dql_src_dir
            .join("tree_sitter")
            .join("parser.h")
            .display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        grammar_dql_src_dir.join("grammar.json").display()
    );
    println!("cargo:rerun-if-changed={}", grammar_rules_js.display());
    println!(
        "cargo:rerun-if-changed={}",
        grammar_rules_parser_c.display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        grammar_rules_src_dir
            .join("tree_sitter")
            .join("parser.h")
            .display()
    );
    println!("cargo:rerun-if-changed={}", grammar_ddl_js.display());
    println!("cargo:rerun-if-changed={}", grammar_ddl_parser_c.display());
    println!(
        "cargo:rerun-if-changed={}",
        grammar_ddl_src_dir
            .join("tree_sitter")
            .join("parser.h")
            .display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        grammar_ddl_src_dir.join("grammar.json").display()
    );

    // Check if grammar.js is newer than parser.c (or if parser.c or headers don't exist)
    let dql_parser_h = grammar_dql_src_dir.join("tree_sitter").join("parser.h");
    let should_regenerate = if !grammar_dql_parser_c.exists() || !dql_parser_h.exists() {
        println!(
            "cargo:warning=DQL parser.c or tree_sitter headers missing, generating from grammar.js"
        );
        true
    } else {
        let grammar_modified = std::fs::metadata(&grammar_dql_js)
            .expect("Failed to get grammar.js metadata")
            .modified()
            .expect("Failed to get grammar.js modified time");

        let parser_modified = std::fs::metadata(&grammar_dql_parser_c)
            .expect("Failed to get parser.c metadata")
            .modified()
            .expect("Failed to get parser.c modified time");

        if grammar_modified > parser_modified {
            println!("cargo:warning=grammar.js is newer than parser.c, regenerating parser");
            true
        } else {
            false
        }
    };

    // Regenerate DQL parser if needed
    if should_regenerate {
        println!("cargo:warning=Running tree-sitter generate for DQL grammar...");

        // Use Makefile to ensure tree-sitter CLI is installed with correct version
        // Then generate the parser
        let make_result = Command::new("make")
            .args(["generate-parser"])
            .current_dir(&workspace_root)
            .output();

        match make_result {
            Ok(output) if output.status.success() => {
                println!("cargo:warning=Successfully regenerated DQL parser via Makefile");
            }
            Ok(output) => {
                eprintln!(
                    "make generate-parser failed:\n{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                panic!(
                    "\n\n\
                    Failed to generate parser!\n\
                    \n\
                    Run this manually to see the error:\n\
                      make generate-parser\n\
                    \n\
                    Or install tree-sitter CLI:\n\
                      make ensure-tree-sitter\n\
                    "
                );
            }
            Err(e) => {
                panic!(
                    "\n\n\
                    Failed to run make: {}\n\
                    \n\
                    Ensure you have make installed and run:\n\
                      make generate-parser\n\
                    ",
                    e
                );
            }
        }
    }

    // Regenerate rules parser if needed
    // Rules grammar extends DQL grammar via require('../grammar_dql/grammar'),
    // so changes to grammar_dql/grammar.js also invalidate grammar_rules/src/parser.c.
    let rules_parser_h = grammar_rules_src_dir.join("tree_sitter").join("parser.h");
    let should_regenerate_rules = if !grammar_rules_parser_c.exists() || !rules_parser_h.exists() {
        println!("cargo:warning=Rules parser.c or tree_sitter headers missing, generating from grammar.js");
        true
    } else if let (Ok(rules_js_meta), Ok(dql_js_meta), Ok(c_meta)) = (
        std::fs::metadata(&grammar_rules_js),
        std::fs::metadata(&grammar_dql_js),
        std::fs::metadata(&grammar_rules_parser_c),
    ) {
        let rules_js_mod = rules_js_meta.modified().unwrap();
        let dql_js_mod = dql_js_meta.modified().unwrap();
        let c_mod = c_meta.modified().unwrap();
        // Regenerate if either the rules grammar.js or the base DQL grammar.js is newer
        let newest_source = std::cmp::max(rules_js_mod, dql_js_mod);
        if newest_source > c_mod {
            println!("cargo:warning=Rules grammar source (own or DQL base) is newer than parser.c, regenerating");
            true
        } else {
            false
        }
    } else {
        false
    };

    if should_regenerate_rules {
        println!("cargo:warning=Running tree-sitter generate for rules grammar...");
        let result = Command::new("tree-sitter")
            .args(["generate"])
            .current_dir(&grammar_rules_dir)
            .output();

        match result {
            Ok(output) if output.status.success() => {
                println!("cargo:warning=Successfully regenerated rules parser");
            }
            Ok(output) => {
                eprintln!(
                    "tree-sitter generate (rules) failed:\n{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                panic!("Failed to generate rules parser! Run: cd grammar_rules && tree-sitter generate");
            }
            Err(e) => {
                panic!("Failed to run tree-sitter for rules grammar: {}", e);
            }
        }
    }

    // Regenerate DDL parser if needed
    // DDL grammar extends DQL grammar via require('../grammar_dql/grammar'),
    // so changes to grammar_dql/grammar.js also invalidate grammar_ddl/src/parser.c.
    let ddl_parser_h = grammar_ddl_src_dir.join("tree_sitter").join("parser.h");
    let should_regenerate_ddl = if !grammar_ddl_parser_c.exists() || !ddl_parser_h.exists() {
        println!(
            "cargo:warning=DDL parser.c or tree_sitter headers missing, generating from grammar.js"
        );
        true
    } else if let (Ok(ddl_js_meta), Ok(dql_js_meta), Ok(c_meta)) = (
        std::fs::metadata(&grammar_ddl_js),
        std::fs::metadata(&grammar_dql_js),
        std::fs::metadata(&grammar_ddl_parser_c),
    ) {
        let ddl_js_mod = ddl_js_meta.modified().unwrap();
        let dql_js_mod = dql_js_meta.modified().unwrap();
        let c_mod = c_meta.modified().unwrap();
        // Regenerate if either the DDL grammar.js or the base DQL grammar.js is newer
        let newest_source = std::cmp::max(ddl_js_mod, dql_js_mod);
        if newest_source > c_mod {
            println!("cargo:warning=DDL grammar source (own or DQL base) is newer than parser.c, regenerating");
            true
        } else {
            false
        }
    } else {
        false
    };

    if should_regenerate_ddl {
        println!("cargo:warning=Running tree-sitter generate for DDL grammar...");
        let result = Command::new("tree-sitter")
            .args(["generate"])
            .current_dir(&grammar_ddl_dir)
            .output();

        match result {
            Ok(output) if output.status.success() => {
                println!("cargo:warning=Successfully regenerated DDL parser");
            }
            Ok(output) => {
                eprintln!(
                    "tree-sitter generate (DDL) failed:\n{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                panic!(
                    "Failed to generate DDL parser! Run: cd grammar_ddl && tree-sitter generate"
                );
            }
            Err(e) => {
                panic!("Failed to run tree-sitter for DDL grammar: {}", e);
            }
        }
    }

    // Get target to determine compilation strategy
    let target = std::env::var("TARGET").unwrap();

    if target.contains("wasm32") {
        // For WASM: Use minimal stdlib.h stub to compile parser.c
        // tree-sitter-c2rust's pure Rust runtime handles everything else (no C library needed!)
        println!("cargo:warning=Compiling parser.c for WASM with minimal headers");
        println!("cargo:warning=Using tree-sitter-c2rust pure Rust runtime");

        let wasm_headers_dir = manifest_dir.join("wasm_headers");
        println!("cargo:warning=WASM headers: {}", wasm_headers_dir.display());

        // DQL parser
        cc::Build::new()
            .include(&grammar_dql_src_dir)
            .include(&wasm_headers_dir)
            .file(&grammar_dql_parser_c)
            .compile("tree-sitter-delightql-v2");

        // Rules parser
        cc::Build::new()
            .include(&grammar_rules_src_dir)
            .include(&wasm_headers_dir)
            .file(&grammar_rules_parser_c)
            .compile("tree-sitter-delightql-rules");

        // DDL parser (sigil sub-language)
        cc::Build::new()
            .include(&grammar_ddl_src_dir)
            .include(&wasm_headers_dir)
            .file(&grammar_ddl_parser_c)
            .compile("tree-sitter-delightql-ddl");

        println!("cargo:warning=Successfully compiled DQL + rules + DDL parsers for WASM");
    } else {
        // For native: Simple compilation, tree-sitter-c2rust runtime handles the rest
        println!("cargo:warning=Compiling DQL + rules + DDL parsers for native with tree-sitter-c2rust runtime");

        // DQL parser
        cc::Build::new()
            .include(&grammar_dql_src_dir)
            .file(&grammar_dql_parser_c)
            .compile("tree-sitter-delightql-v2");

        // Rules parser
        cc::Build::new()
            .include(&grammar_rules_src_dir)
            .file(&grammar_rules_parser_c)
            .compile("tree-sitter-delightql-rules");

        // DDL parser (sigil sub-language)
        cc::Build::new()
            .include(&grammar_ddl_src_dir)
            .file(&grammar_ddl_parser_c)
            .compile("tree-sitter-delightql-ddl");

        println!("cargo:warning=Successfully compiled DQL + rules + DDL parsers for native");
    }
}

/// Walk all top-level directories under autoload/ and generate src/stdlib_manifest.rs
/// with include_str!() entries. The directory name becomes the namespace prefix.
///
/// Directory structure mirrors namespaces directly:
///   autoload/std/info.dql            -> std::info
///   autoload/std/help.dql            -> std::help
///   autoload/std/util/string.dql     -> std::util::string
///   autoload/sys/build.dql           -> sys::build
///
/// Files already handled by dedicated loaders (sys/meta.dql) are excluded.
fn generate_stdlib_manifest(manifest_dir: &std::path::Path) {
    let autoload_dir = manifest_dir.join("autoload");
    let manifest_path = manifest_dir.join("src").join("stdlib_manifest.rs");

    // Rerun if anything under autoload/ changes
    println!("cargo:rerun-if-changed={}", autoload_dir.display());

    // Files with dedicated loaders — excluded from the manifest
    let excluded: &[&str] = &["sys::meta"];

    let mut modules: Vec<(String, PathBuf)> = Vec::new();

    if autoload_dir.exists() {
        // Walk each top-level subdirectory (std, sys, ...)
        if let Ok(entries) = std::fs::read_dir(&autoload_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    let prefix = path.file_name().unwrap().to_string_lossy().to_string();
                    collect_dql_files(&path, &path, &prefix, &mut modules);
                }
            }
        }
        // Remove excluded modules
        modules.retain(|(ns, _)| !excluded.contains(&ns.as_str()));
        modules.sort_by(|a, b| a.0.cmp(&b.0));
    }

    // Build content in memory first (write-if-changed pattern)
    let mut content = String::new();
    use std::fmt::Write as FmtWrite;
    writeln!(content, "// AUTO-GENERATED by build.rs -- do not edit").unwrap();
    writeln!(content, "pub const STDLIB_MODULES: &[(&str, &str)] = &[").unwrap();
    for (namespace, path) in &modules {
        // Path relative to manifest_dir for include_str!()
        let rel = path
            .strip_prefix(manifest_dir)
            .expect("file not under manifest dir");
        // include_str! paths are relative to the source file (src/stdlib_manifest.rs),
        // so we need ../ to get back to crate root
        let include_path = format!("../{}", rel.display());
        writeln!(
            content,
            "    (\"{}\", include_str!(\"{}\")),",
            namespace, include_path
        )
        .unwrap();

        // Track individual files for precise rerun triggers
        println!("cargo:rerun-if-changed={}", path.display());
    }
    writeln!(content, "];").unwrap();

    // Only write if content changed — prevents cross-platform formatting churn
    let existing = std::fs::read_to_string(&manifest_path).unwrap_or_default();
    if existing != content {
        std::fs::write(&manifest_path, &content).expect("Failed to write stdlib_manifest.rs");
        println!(
            "cargo:warning=Generated stdlib manifest with {} module(s)",
            modules.len()
        );
    }
}

/// Recursively collect .dql files under `base_dir`, computing namespaces from paths.
/// `prefix` is the top-level namespace (e.g. "std", "sys").
fn collect_dql_files(
    dir: &std::path::Path,
    base_dir: &std::path::Path,
    prefix: &str,
    modules: &mut Vec<(String, PathBuf)>,
) {
    // Watch this directory so new/removed files trigger a rebuild
    println!("cargo:rerun-if-changed={}", dir.display());

    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_dql_files(&path, base_dir, prefix, modules);
        } else if path.extension().map_or(false, |ext| ext == "dql") {
            // Compute namespace: strip base_dir prefix, strip .dql extension,
            // replace path separators with ::, prepend prefix::
            let rel = path
                .strip_prefix(base_dir)
                .expect("file not under base dir");
            let stem = rel.with_extension("");
            let ns_path = stem
                .components()
                .map(|c| c.as_os_str().to_string_lossy().to_string())
                .collect::<Vec<_>>()
                .join("::");
            let namespace = format!("{}::{}", prefix, ns_path);
            modules.push((namespace, path.clone()));
        }
    }
}
