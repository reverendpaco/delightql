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
# it. The CLI version equals the `tree-sitter` runtime version in Cargo.toml,
# and it is installed with `--locked`: an unlocked install links whatever
# runtime and generator the registry resolves that day, and the version the
# binary prints is then not the version that generated the parser.
TREE_SITTER_EXPECTED_VERSION := 0.27.0
# A clang with the wasm32 backend, for compiling the generated parser.c to
# wasm32-unknown-unknown. Any clang built with the WebAssembly target serves;
# Apple's system clang lacks it, so macOS points this at Homebrew's LLVM.
WASM_CLANG ?= $(shell command -v clang 2>/dev/null)
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
	@if [ -z "$(WASM_CLANG)" ] || ! $(WASM_CLANG) --print-targets 2>/dev/null | grep -q wasm32; then \
		echo "❌ no clang with the wasm32 backend (WASM_CLANG=$(WASM_CLANG))"; \
		echo "   Linux: the distribution clang; macOS: brew install llvm and WASM_CLANG=/opt/homebrew/opt/llvm/bin/clang"; \
		exit 1; \
	else \
		echo "✓ wasm32-capable clang at $(WASM_CLANG)"; \
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
		cargo install --locked tree-sitter-cli --version $(TREE_SITTER_EXPECTED_VERSION); \
	else \
		INSTALLED_VERSION=$$(tree-sitter --version 2>&1 | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1); \
		if [ "$$INSTALLED_VERSION" != "$(TREE_SITTER_EXPECTED_VERSION)" ]; then \
			echo "❌ tree-sitter CLI $$INSTALLED_VERSION is on PATH; the pin is $(TREE_SITTER_EXPECTED_VERSION)"; \
			echo "   cargo install --locked tree-sitter-cli --version $(TREE_SITTER_EXPECTED_VERSION) --force"; \
			exit 1; \
		fi; \
		echo "✓ tree-sitter CLI $$INSTALLED_VERSION (the pin; install it --locked so the generator it links is the release's own)"; \
	fi

.PHONY: generate-grammar
# THE GRAMMAR'S ONE GENERATION CONTRACT: the pinned CLI named here, enforced
# (not hinted) by delightql-cst's build.rs, writing only into ignored paths.
# This target exists for humans; the crate build does not shell out to make.
generate-grammar: ensure-tree-sitter
	@INSTALLED_VERSION=$$(tree-sitter --version 2>&1 | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1); \
	if [ "$$INSTALLED_VERSION" != "$(TREE_SITTER_EXPECTED_VERSION)" ]; then \
		echo "❌ tree-sitter CLI $$INSTALLED_VERSION; the pin is $(TREE_SITTER_EXPECTED_VERSION)"; \
		echo "   cargo install --locked tree-sitter-cli --version $(TREE_SITTER_EXPECTED_VERSION) --force"; \
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


# --- cross-compiled release tarballs -----------------------------------------
# `make dist` builds every target this host can build and packages each as
# dist/dql-<version>-<platform>.tar.gz, with dist/SHA256SUMS. The Linux
# targets are cross-compiled with cargo-zigbuild, which uses zig as the C
# compiler and linker for the bundled SQLite and tree-sitter C sources, so
# they build from Linux or macOS alike. macOS builds only on macOS: linking
# needs Apple's SDK, which is licensed for Apple hardware.
#
# The musl builds are fully static and run on any Linux distribution. The
# glibc build targets glibc 2.17 (zig selects the version from the suffix),
# so it runs on anything newer.
DIST_DIR        := dist
DIST_TARGET_DIR := target/dist
DIST_PROFILE    := release-ship
DIST_LINUX      := x86_64-unknown-linux-musl aarch64-unknown-linux-musl aarch64-unknown-linux-gnu
DIST_MACOS      := aarch64-apple-darwin x86_64-apple-darwin
DIST_VERSION     = $(shell cargo pkgid -p delightql-cli | sed 's/.*[#@]//')
HOST_OS         := $(shell uname -s)

.PHONY: dist dist-setup dist-linux dist-macos dist-clean ensure-zig ensure-zigbuild

dist: dist-linux dist-macos
	@cd $(DIST_DIR) && (command -v sha256sum >/dev/null 2>&1 && sha256sum dql-*.tar.gz || shasum -a 256 dql-*.tar.gz) > SHA256SUMS
	@echo ""
	@echo "✓ $(DIST_DIR)/:"
	@cat $(DIST_DIR)/SHA256SUMS

# One-time: the Rust targets and cargo-zigbuild. zig itself comes from the
# system package manager, `mise install`, or `pip install ziglang`.
dist-setup: ensure-cargo ensure-zig
	rustup target add $(DIST_LINUX) $(if $(filter Darwin,$(HOST_OS)),$(DIST_MACOS))
	@command -v cargo-zigbuild >/dev/null 2>&1 || cargo install --locked cargo-zigbuild

dist-linux: ensure-cargo ensure-uv ensure-tree-sitter ensure-zig ensure-zigbuild
	@mkdir -p $(DIST_DIR)
	@set -e; for t in $(DIST_LINUX); do \
		zt=$$t; [ $$t = aarch64-unknown-linux-gnu ] && zt=$$t.2.17; \
		echo "--- $$t"; \
		CARGO_TARGET_DIR=$(DIST_TARGET_DIR) cargo zigbuild --profile $(DIST_PROFILE) --bin dql --target $$zt; \
		$(MAKE) --no-print-directory dist-pack BIN=$(DIST_TARGET_DIR)/$$t/$(DIST_PROFILE)/dql \
			PLATFORM=$$(echo $$t | sed 's/-unknown//'); \
	done

ifeq ($(HOST_OS),Darwin)
dist-macos: ensure-cargo ensure-uv ensure-tree-sitter
	@mkdir -p $(DIST_DIR) $(DIST_TARGET_DIR)/universal
	@set -e; for t in $(DIST_MACOS); do \
		echo "--- $$t"; \
		CARGO_TARGET_DIR=$(DIST_TARGET_DIR) cargo build --profile $(DIST_PROFILE) --bin dql --target $$t; \
	done
	lipo -create -output $(DIST_TARGET_DIR)/universal/dql \
		$(foreach t,$(DIST_MACOS),$(DIST_TARGET_DIR)/$(t)/$(DIST_PROFILE)/dql)
	codesign --force --sign - $(DIST_TARGET_DIR)/universal/dql
	@$(MAKE) --no-print-directory dist-pack BIN=$(DIST_TARGET_DIR)/universal/dql PLATFORM=macos-universal
else
dist-macos:
	@echo "--- macOS: skipped (needs a Mac: linking requires Apple's SDK)"
endif

# Package one binary: dql, LICENSE and README.md under dql-<version>-<platform>/.
.PHONY: dist-pack
dist-pack:
	@name=dql-$(DIST_VERSION)-$(PLATFORM); stage=$(DIST_TARGET_DIR)/stage/$$name; \
	rm -rf $$stage && mkdir -p $$stage && \
	cp $(BIN) LICENSE README.md $$stage/ && \
	tar -C $(DIST_TARGET_DIR)/stage -czf $(DIST_DIR)/$$name.tar.gz $$name && \
	echo "✓ $(DIST_DIR)/$$name.tar.gz"

dist-clean:
	rm -rf $(DIST_DIR) $(DIST_TARGET_DIR)

ensure-zig:
	@if ! command -v zig >/dev/null 2>&1 && ! python3 -c 'import ziglang' 2>/dev/null; then \
		echo "❌ zig not found (cargo-zigbuild uses it to cross-compile the Linux targets)."; \
		echo "   Install: mise install   or   your package manager   or   pip install ziglang"; \
		exit 1; \
	fi

ensure-zigbuild:
	@if ! command -v cargo-zigbuild >/dev/null 2>&1; then \
		echo "❌ cargo-zigbuild not found. Run: make dist-setup"; \
		exit 1; \
	fi


help:
	@echo "DelightQL Dependency Management"
	@echo ""
	@echo "Targets:"
	@echo "  make [build]           - Check cargo+uv, build dql -> target/debug/dql"
	@echo "  make grammar-fields    - Refuse grammar fields without Rust readers"
	@echo "  make error-expectations - Ratchet empty and bare refusal expectations"
	@echo "  make ship              - Optimized build (fat LTO, stripped) -> target/release-ship/dql"
	@echo "  make dist              - Release tarballs for every platform this host can build -> dist/"
	@echo "  make dist-setup        - One-time: Rust targets + cargo-zigbuild (zig itself: mise/pkg/pip)"
	@echo "  make dist-linux        - Only the three Linux tarballs (x86_64/aarch64 musl, aarch64 glibc)"
	@echo "  make dist-macos        - Only the macOS universal tarball (skipped off macOS)"
	@echo "  make setup             - Ensure all build dependencies are installed"
	@echo "  make ensure-tree-sitter - Ensure tree-sitter CLI is installed (pinned to $(TREE_SITTER_EXPECTED_VERSION))"
	@echo "  make generate-grammar  - Generate the parser from grammar.js (derived, ignored)"
	@echo "  make help              - Show this help"
	@echo ""
	@echo "Individual dependency checks:"
	@echo "  make ensure-rust       - Check Rust + wasm32 target"
	@echo "  make ensure-llvm       - Check for a clang with the wasm32 backend"
	@echo "  make ensure-duckdb     - Check DuckDB"
	@echo "  make ensure-wasm-pack  - Check wasm-pack"
	@echo "  make ensure-node       - Check Node.js"
