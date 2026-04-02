# The Delightql Query Language

# Documentation

See delightql.org


# Crates

  - `./crates/delightql-core/`: Compilation pipeline (parser, resolver, transformer, refiner) and SQL generation.
  - `./crates/delightql-cli/`: REPL, CLI commands, and the `dql` binary.
  - `./crates/delightql-backends/`: Database backend implementations (schema introspection, connection management, query execution).
  - `./crates/delightql-sqlite-relay/`: SQLite execution engine with cursor-based streaming via the protocol layer.
  - `./crates/delightql-postgres/`: PostgreSQL driver implementation.
  - `./crates/delightql-cli-siso/`: Pipe-based connections to external CLI tools (osqueryi, sqlite3) via stdin/stdout.
  - `./crates/delightql-cabi/`: C-ABI shared library for FFI from Python, Swift, Go, etc.
  - `./crates/delightql-wasm/`: WebAssembly bindings bridging the Rust engine to JavaScript's sqlite3-wasm.
  - `./crates/delightql-types/`: Shared types used across crates to break circular dependencies.
  - `./crates/delightql-macros/`: Proc macros (derive `ToLispy`, `PhaseConvert`).
  - `./crates/delightql-formatter/`: Tree-sitter-based source code formatter.

# Building

Make sure you have tree-sitter version installed 0.25.2
and then:

`cargo build --bin dql`

# License

Apache 2.0.   See LICENSE

