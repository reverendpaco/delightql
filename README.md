# The Delightql Query Language

# Documentation

See delightql.org



# Building

Requirements: **rustc/cargo** ([rustup.rs](https://rustup.rs)), **make**,
and **uv** ([docs.astral.sh/uv](https://docs.astral.sh/uv/)) — build.rs
bundles the embedded documentation databases from `assets/`, and the
bundler runs under uv (it declares its own python dependencies). Then:

`make`

leaves the binary at `target/debug/dql`. The parser is generated from
`grammar/` at build time, not checked in, so the first `make` also installs
the pinned tree-sitter CLI into `.tools/` (one compile, a few minutes); a
`tree-sitter` already on your PATH is neither used nor touched. After that
first `make`, `cargo build --bin dql` works directly too.

`make ship` builds the optimized profile — fat LTO, stripped — to
`target/release-ship/dql`. It takes considerably longer, and the stripped
symbols make panics less legible, so it is for producing a deliverable
rather than for working on one.

`make setup` is the dependency doctor for the wider toolchain (libduckdb
for the DuckDB engine, node for the Node.js binding).


# License

Apache 2.0.   See LICENSE

