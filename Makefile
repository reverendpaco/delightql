# DelightQL Dependency Management
# Idempotent targets for ensuring build dependencies are present

# Tree-sitter CLI must match tree-sitter-c2rust version in Cargo.toml
TREE_SITTER_EXPECTED_VERSION := 0.25.2
LLVM_PATH := /opt/homebrew/opt/llvm/bin/clang
DUCKDB_LIB := /opt/homebrew/lib/libduckdb.dylib

.PHONY: setup
setup: ensure-rust ensure-llvm ensure-duckdb ensure-wasm-pack ensure-node ensure-tree-sitter
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

.PHONY: generate-parser
generate-parser: ensure-tree-sitter
	@echo "Checking tree-sitter CLI version..."
	@INSTALLED_VERSION=$$(tree-sitter --version 2>&1 | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1); \
	if [ "$$INSTALLED_VERSION" != "$(TREE_SITTER_EXPECTED_VERSION)" ]; then \
		echo ""; \
		echo "❌ ERROR: Wrong tree-sitter CLI version!"; \
		echo ""; \
		echo "   Expected: $(TREE_SITTER_EXPECTED_VERSION) (must match tree-sitter-c2rust)"; \
		echo "   Found:    $$INSTALLED_VERSION"; \
		echo ""; \
		echo "   The parser.c file must be generated with tree-sitter $(TREE_SITTER_EXPECTED_VERSION)"; \
		echo "   to match the tree-sitter-c2rust crate version for ABI compatibility."; \
		echo ""; \
		echo "   To fix this, reinstall the correct version:"; \
		echo "     cargo install tree-sitter-cli --version $(TREE_SITTER_EXPECTED_VERSION) --force"; \
		echo ""; \
		exit 1; \
	fi
	@echo "✓ tree-sitter CLI version $(TREE_SITTER_EXPECTED_VERSION) confirmed"
	@echo "Generating parser from grammar.js..."
	@cd grammar_dql && tree-sitter generate
	@echo "✓ Parser generated"

.PHONY: help
help:
	@echo "DelightQL Dependency Management"
	@echo ""
	@echo "Targets:"
	@echo "  make setup             - Ensure all build dependencies are installed"
	@echo "  make ensure-tree-sitter - Ensure tree-sitter CLI is installed (pinned to $(TREE_SITTER_REV))"
	@echo "  make generate-parser   - Generate parser.c from grammar.js"
	@echo "  make help              - Show this help"
	@echo ""
	@echo "Individual dependency checks:"
	@echo "  make ensure-rust       - Check Rust + wasm32 target"
	@echo "  make ensure-llvm       - Check LLVM clang"
	@echo "  make ensure-duckdb     - Check DuckDB"
	@echo "  make ensure-wasm-pack  - Check wasm-pack"
	@echo "  make ensure-node       - Check Node.js"
