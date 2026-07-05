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

# Python script conventions

Three tiers, decided by how a file is used. The shebang line tells you
which world a script lives in:

1. **Executable with external packages** — `uv run` shebang plus a
   PEP 723 inline metadata block declaring the dependencies (and
   `requires-python`). No venv ceremony; uv provides the environment.

   ```python
   #!/usr/bin/env -S uv run --quiet --script
   # /// script
   # requires-python = ">=3.10"
   # dependencies = ["pyyaml"]
   # ///
   ```

   Examples: `scripts/generate_tutorial_db.py`,
   `new_test_suite/test_cabi.py`, `new_test_suite/features/scan.py`.

2. **Executable, stdlib-only** — plain `#!/usr/bin/env python3`.
   Deliberately NOT uv: these run anywhere a python3 exists, and the
   test suite's inner loop (`test.sh` → `pack.py`) must not acquire a
   uv/network dependency for zero isolation benefit.

   Examples: `new_test_suite/pack.py`, `run-one.py`, `sweep.py`.

3. **Imported modules** — no shebang (they're not entry points).

   Examples: `book/docs-site/delightql_lexer.py`, `hosts/python/delightql/`.

When a tier-2 script grows its first external dependency, it moves to
tier 1 (shebang + PEP 723 block), and any Makefile/script invoking it
as `python3 file.py` switches to invoking it directly via the shebang.

# License

Apache 2.0.   See LICENSE

