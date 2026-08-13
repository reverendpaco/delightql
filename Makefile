# DelightQL Dependency Management + clone-and-build entry point
#
# `make` (or `make build`) checks the tools cargo cannot provide itself
# and builds the dql binary. Compiling requires: rustc/cargo, make (you
# are running it), and uv — build.rs bundles the embedded book/man
# databases via assets/Makefile, whose bundler runs under `uv run` and
# declares its own python dependencies (PEP 723). Everything else here
# is the optional dependency doctor (`make setup`) for the wider
# toolchain (wasm, duckdb, tree-sitter regeneration).

# The grammar is generated at build time from ignored paths, so
# compiling delightql-core requires the pinned CLI — `build` and `ship` ensure
# it. Tree-sitter CLI must match tree-sitter-c2rust version in Cargo.toml
TREE_SITTER_EXPECTED_VERSION := 0.25.2
LLVM_PATH := /opt/homebrew/opt/llvm/bin/clang
DUCKDB_LIB := /opt/homebrew/lib/libduckdb.dylib

.DEFAULT_GOAL := build

# NOT part of the routine per-bump check. Clippy re-checks all of
# delightql-core on any edit and --all-targets adds every test target, so
# this is minutes, not seconds. It has its own CARGO_TARGET_DIR (see
# lint_ratchet.py) so it does not evict the debug build's artifacts —
# but run it when a change could add a lint class, not reflexively.
.PHONY: lint build grammar-fields error-expectations
# The default is the CLONER's build: fast to produce, symbols intact, panics
# legible. `make ship` is for producing the deliverable, not for meeting the
# project.
build: ensure-cargo ensure-uv ensure-tree-sitter grammar-fields error-expectations
	cargo build --bin dql
	@echo ""
	@echo "✓ built: target/debug/dql"

grammar-fields:
	@./grammar_field_check.py

error-expectations:
	@./error_expectation_check.py

.PHONY: ship
ship: ensure-cargo ensure-uv ensure-tree-sitter grammar-fields error-expectations
	cargo build --profile release-ship --bin dql
	@echo ""
	@echo "✓ built: target/release-ship/dql  (optimized, fat LTO, stripped)"

.PHONY: ensure-cargo
ensure-cargo:
	@if ! command -v cargo >/dev/null 2>&1; then \
		echo "❌ cargo not found. Install from: https://rustup.rs"; \
		exit 1; \
	fi

.PHONY: ensure-uv
ensure-uv:
	@if ! command -v uv >/dev/null 2>&1; then \
		echo "❌ uv not found (the asset bundler runs under it; see assets/Makefile)."; \
		echo "   Install: mise install   or   https://docs.astral.sh/uv/"; \
		exit 1; \
	fi

.PHONY: setup
setup: ensure-rust ensure-uv ensure-llvm ensure-duckdb ensure-wasm-pack ensure-node ensure-tree-sitter
	@echo ""
	@echo "✅ All dependencies ready"
	@echo ""
	@echo "Next steps:"
	@echo "  cargo build --bin dql"
	@echo "  cd crates/delightql-wasm && make build"

.PHONY: ensure-rust
ensure-rust:
	@if ! command -v rustc >/dev/null 2>&1; then \
		echo "❌ Rust not found. Install from: https://rustup.rs"; \
		exit 1; \
	else \
		echo "✓ Rust $(shell rustc --version)"; \
	fi
	@if ! rustup target list --installed | grep -q wasm32-unknown-unknown; then \
		echo "  Installing wasm32-unknown-unknown target..."; \
		rustup target add wasm32-unknown-unknown; \
	else \
		echo "✓ wasm32-unknown-unknown target installed"; \
	fi

.PHONY: ensure-llvm
ensure-llvm:
	@if [ ! -f $(LLVM_PATH) ]; then \
		echo "Installing LLVM (needed for WASM C compilation)..."; \
		brew install llvm; \
	else \
		echo "✓ LLVM clang at $(LLVM_PATH)"; \
	fi

.PHONY: ensure-duckdb
ensure-duckdb:
	@if [ ! -f $(DUCKDB_LIB) ]; then \
		echo "Installing DuckDB..."; \
		brew install duckdb; \
	else \
		echo "✓ DuckDB at $(DUCKDB_LIB)"; \
	fi

.PHONY: ensure-wasm-pack
ensure-wasm-pack:
	@if ! command -v wasm-pack >/dev/null 2>&1; then \
		echo "Installing wasm-pack..."; \
		cargo install wasm-pack; \
	else \
		echo "✓ wasm-pack $(shell wasm-pack --version)"; \
	fi

.PHONY: ensure-node
ensure-node:
	@if ! command -v node >/dev/null 2>&1; then \
		echo "Installing Node.js..."; \
		if command -v mise >/dev/null 2>&1; then \
			mise install node; \
		else \
			brew install node; \
		fi; \
	else \
		echo "✓ Node.js $(shell node --version)"; \
	fi

.PHONY: ensure-tree-sitter
ensure-tree-sitter:
	@if ! command -v tree-sitter >/dev/null 2>&1; then \
		echo "Installing tree-sitter CLI v$(TREE_SITTER_EXPECTED_VERSION)..."; \
		cargo install tree-sitter-cli --version $(TREE_SITTER_EXPECTED_VERSION); \
	else \
		INSTALLED_VERSION=$$(tree-sitter --version 2>&1 | grep -o 'tree-sitter [0-9.]*' || echo "unknown"); \
		echo "✓ tree-sitter CLI installed ($$INSTALLED_VERSION)"; \
		echo "  Note: Must be v$(TREE_SITTER_EXPECTED_VERSION) to match tree-sitter-c2rust in Cargo.toml"; \
	fi

.PHONY: generate-grammar
# THE GRAMMAR'S ONE GENERATION CONTRACT: the pinned CLI named here, enforced
# (not hinted) by delightql-cst's build.rs, writing only into ignored paths.
# This target exists for humans; the crate build does not shell out to make.
generate-grammar: ensure-tree-sitter
	@INSTALLED_VERSION=$$(tree-sitter --version 2>&1 | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1); \
	if [ "$$INSTALLED_VERSION" != "$(TREE_SITTER_EXPECTED_VERSION)" ]; then \
		echo "❌ tree-sitter CLI $$INSTALLED_VERSION; the pin is $(TREE_SITTER_EXPECTED_VERSION)"; \
		echo "   cargo install tree-sitter-cli --version $(TREE_SITTER_EXPECTED_VERSION) --force"; \
		exit 1; \
	fi
	@cd grammar && tree-sitter generate
	@echo "✓ grammar generated (derived output stays ignored)"

.PHONY: help
# The pipeline's lint directives are clippy-only and had never run: clippy
# hard-failed in delightql-formatter before reaching core. It runs now, and
# its findings are ratcheted rather than paid down — the count may fall,
# never rise. See lint_ratchet.py for why neither weakening the directives
# nor a 549-site burndown is the answer.
lint: grammar-fields error-expectations
	@./lint_ratchet.py


help:
	@echo "DelightQL Dependency Management"
	@echo ""
	@echo "Targets:"
	@echo "  make [build]           - Check cargo+uv, build dql -> target/debug/dql"
	@echo "  make grammar-fields    - Refuse grammar fields without Rust readers"
	@echo "  make error-expectations - Ratchet empty and bare refusal expectations"
	@echo "  make ship              - Optimized build (fat LTO, stripped) -> target/release-ship/dql"
	@echo "  make setup             - Ensure all build dependencies are installed"
	@echo "  make ensure-tree-sitter - Ensure tree-sitter CLI is installed (pinned to $(TREE_SITTER_EXPECTED_VERSION))"
	@echo "  make generate-grammar  - Generate the parser from grammar.js (derived, ignored)"
	@echo "  make help              - Show this help"
	@echo ""
	@echo "Individual dependency checks:"
	@echo "  make ensure-rust       - Check Rust + wasm32 target"
	@echo "  make ensure-llvm       - Check LLVM clang"
	@echo "  make ensure-duckdb     - Check DuckDB"
	@echo "  make ensure-wasm-pack  - Check wasm-pack"
	@echo "  make ensure-node       - Check Node.js"
