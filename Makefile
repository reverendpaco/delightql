# DelightQL Dependency Management + clone-and-build entry point
#
# `make` (or `make build`) checks the tools cargo cannot provide itself,
# installs the pinned tree-sitter CLI into the checkout, and builds the dql
# binary. Compiling requires: rustc/cargo, make (you are running it), and
# uv — build.rs bundles the embedded book/man databases via
# assets/Makefile, whose bundler runs under `uv run` and declares its own
# python dependencies (PEP 723). Everything else here is the optional
# dependency doctor (`make setup`) for the wider toolchain (duckdb, node).

# The grammar is generated at build time from ignored paths, so
# compiling delightql-core requires the pinned CLI — `build` and `ship` ensure
# it. The CLI version equals the `tree-sitter` runtime version in Cargo.toml,
# and it is installed with `--locked`: an unlocked install links whatever
# runtime and generator the registry resolves that day, and the version the
# binary prints is then not the version that generated the parser.
#
# It is installed under TOOLS_ROOT, where every pinned tool this Makefile
# installs lives, and delightql-cst's build.rs reads both assignments here
# and runs exactly $(TREE_SITTER). A `tree-sitter` found on PATH is never
# used: it may be any version, or an unlocked build that prints the pinned
# one.
TREE_SITTER_EXPECTED_VERSION := 0.27.0
TOOLS_ROOT := .tools
TREE_SITTER = $(TOOLS_ROOT)/bin/tree-sitter

.DEFAULT_GOAL := build

.PHONY: build
# The default is the CLONER's build: fast to produce, symbols intact, panics
# legible. `make ship` is for producing the deliverable, not for meeting the
# project.
build: ensure-cargo ensure-uv ensure-tree-sitter
	cargo build --bin dql
	@echo ""
	@echo "✓ built: target/debug/dql"

.PHONY: ship
ship: ensure-cargo ensure-uv ensure-tree-sitter
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
		echo "   Install: https://docs.astral.sh/uv/getting-started/installation/"; \
		exit 1; \
	fi

.PHONY: setup
setup: ensure-rust ensure-uv ensure-tree-sitter ensure-duckdb ensure-node
	@echo ""
	@echo "✅ All dependencies ready"
	@echo ""
	@echo "Next step:"
	@echo "  make"

.PHONY: ensure-rust
ensure-rust:
	@if ! command -v rustc >/dev/null 2>&1; then \
		echo "❌ Rust not found. Install from: https://rustup.rs"; \
		exit 1; \
	else \
		echo "✓ Rust $(shell rustc --version)"; \
	fi

# Only the duckdb fatboy (`cargo build -p delightql-duckdb`) links libduckdb,
# dynamically. The duckdb crate looks in DUCKDB_LIB_DIR first, then on the
# linker's default paths.
.PHONY: ensure-duckdb
ensure-duckdb:
	@if [ -n "$$DUCKDB_LIB_DIR" ]; then \
		if ls "$$DUCKDB_LIB_DIR"/libduckdb.* >/dev/null 2>&1; then \
			echo "✓ libduckdb in DUCKDB_LIB_DIR=$$DUCKDB_LIB_DIR"; \
		else \
			echo "❌ no libduckdb in DUCKDB_LIB_DIR=$$DUCKDB_LIB_DIR"; \
			exit 1; \
		fi; \
	elif [ -f /opt/homebrew/lib/libduckdb.dylib ] || [ -f /usr/local/lib/libduckdb.dylib ] \
		|| ldconfig -p 2>/dev/null | grep -q 'libduckdb\.so'; then \
		echo "✓ libduckdb on the linker's default path"; \
	elif [ "$$(uname -s)" = Darwin ] && command -v brew >/dev/null 2>&1; then \
		echo "Installing DuckDB..."; \
		brew install duckdb; \
	else \
		echo "❌ libduckdb not found (only the duckdb fatboy needs it)"; \
		echo "   Download libduckdb from https://duckdb.org/docs/installation/ and set DUCKDB_LIB_DIR to its directory"; \
		exit 1; \
	fi

# The node binding (hosts/node) runs under it; building dql does not.
.PHONY: ensure-node
ensure-node:
	@if ! command -v node >/dev/null 2>&1; then \
		echo "❌ node not found (only the node binding needs it). Install: https://nodejs.org/"; \
		exit 1; \
	else \
		echo "✓ Node.js $$(node --version)"; \
	fi

# Installs on first use and again whenever the pin moves; the user's own
# tree-sitter, if any, is left alone.
.PHONY: ensure-tree-sitter
ensure-tree-sitter:
	@INSTALLED_VERSION=$$($(TREE_SITTER) --version 2>/dev/null | awk '{print $$2}'); \
	if [ "$$INSTALLED_VERSION" != "$(TREE_SITTER_EXPECTED_VERSION)" ]; then \
		echo "Installing tree-sitter CLI $(TREE_SITTER_EXPECTED_VERSION) into $(TOOLS_ROOT)/ (one compile, a few minutes)..."; \
		cargo install --locked --force --root $(TOOLS_ROOT) tree-sitter-cli --version $(TREE_SITTER_EXPECTED_VERSION) || exit 1; \
	fi; \
	echo "✓ tree-sitter CLI $(TREE_SITTER_EXPECTED_VERSION) at $(TREE_SITTER)"

.PHONY: generate-grammar
# THE GRAMMAR'S ONE GENERATION CONTRACT: the pinned CLI named here, enforced
# (not hinted) by delightql-cst's build.rs, writing only into ignored paths.
# This target exists for humans; the crate build does not shell out to make.
# `native` evaluates grammar.js in the QuickJS the CLI carries, so generation
# needs no node; build.rs passes it too.
generate-grammar: ensure-tree-sitter
	@cd grammar && $(CURDIR)/$(TREE_SITTER) generate --js-runtime native
	@echo "✓ grammar generated (derived output stays ignored)"


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
#
# Both cross tools are pinned and installed under TOOLS_ROOT on first use:
# cargo-zigbuild with `cargo install --locked`, and zig as the PyPI ziglang
# wheel in a uv venv. cargo-zigbuild asks `python -m ziglang` before any
# `zig` on PATH, and CARGO_ZIGBUILD_PYTHON_PATH names the venv's python, so
# a system zig — any version, or a broken install — is never consulted.
DIST_DIR        := dist
DIST_TARGET_DIR := target/dist
DIST_PROFILE    := release-ship
DIST_LINUX      := x86_64-unknown-linux-musl aarch64-unknown-linux-musl aarch64-unknown-linux-gnu
DIST_MACOS      := aarch64-apple-darwin x86_64-apple-darwin
DIST_VERSION     = $(shell cargo pkgid -p delightql-cli | sed 's/.*[#@]//')
HOST_OS         := $(shell uname -s)
DIST_TARGETS    := $(DIST_LINUX) $(if $(filter Darwin,$(HOST_OS)),$(DIST_MACOS))

CARGO_ZIGBUILD_VERSION := 0.23.4
CARGO_ZIGBUILD          = $(TOOLS_ROOT)/bin/cargo-zigbuild
ZIG_VERSION            := 0.14.1
ZIG_VENV                = $(TOOLS_ROOT)/zig
ZIG_PYTHON              = $(ZIG_VENV)/bin/python

.PHONY: dist dist-setup dist-linux dist-macos dist-clean ensure-zig ensure-zigbuild ensure-dist-targets

dist: dist-linux dist-macos
	@cd $(DIST_DIR) && (command -v sha256sum >/dev/null 2>&1 && sha256sum dql-*.tar.gz || shasum -a 256 dql-*.tar.gz) > SHA256SUMS
	@echo ""
	@echo "✓ $(DIST_DIR)/:"
	@cat $(DIST_DIR)/SHA256SUMS

# Everything `make dist` installs, without building; `make dist` does it too.
dist-setup: ensure-zig ensure-zigbuild ensure-dist-targets

dist-linux: ensure-cargo ensure-uv ensure-tree-sitter ensure-zig ensure-zigbuild ensure-dist-targets
	@mkdir -p $(DIST_DIR)
	@set -e; for t in $(DIST_LINUX); do \
		zt=$$t; [ $$t = aarch64-unknown-linux-gnu ] && zt=$$t.2.17; \
		echo "--- $$t"; \
		CARGO_TARGET_DIR=$(DIST_TARGET_DIR) CARGO_ZIGBUILD_PYTHON_PATH=$(CURDIR)/$(ZIG_PYTHON) \
			$(CURDIR)/$(CARGO_ZIGBUILD) zigbuild --profile $(DIST_PROFILE) --bin dql --target $$zt; \
		$(MAKE) --no-print-directory dist-pack BIN=$(DIST_TARGET_DIR)/$$t/$(DIST_PROFILE)/dql \
			PLATFORM=$$(echo $$t | sed 's/-unknown//'); \
	done

ifeq ($(HOST_OS),Darwin)
dist-macos: ensure-cargo ensure-uv ensure-tree-sitter ensure-dist-targets
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

# Installs on first use, and again whenever the pin moves or the venv stops
# answering (a venv breaks when the python it was made from goes away).
ensure-zig: ensure-uv
	@INSTALLED=$$($(ZIG_PYTHON) -m ziglang version 2>/dev/null); \
	if [ "$$INSTALLED" != "$(ZIG_VERSION)" ]; then \
		echo "Installing zig $(ZIG_VERSION) into $(ZIG_VENV)/ (the PyPI ziglang wheel, about 340 MB unpacked)..."; \
		rm -rf "$(ZIG_VENV)" && uv venv -q "$(ZIG_VENV)" \
			&& uv pip install -q --python "$(ZIG_PYTHON)" ziglang==$(ZIG_VERSION) || exit 1; \
	fi; \
	echo "✓ zig $(ZIG_VERSION) at $(ZIG_VENV)/"

# cargo-zigbuild prints no version, so cargo's own install record answers.
ensure-zigbuild: ensure-cargo
	@if [ ! -x "$(CARGO_ZIGBUILD)" ] \
		|| ! grep -q '^"cargo-zigbuild $(CARGO_ZIGBUILD_VERSION) ' "$(TOOLS_ROOT)/.crates.toml" 2>/dev/null; then \
		echo "Installing cargo-zigbuild $(CARGO_ZIGBUILD_VERSION) into $(TOOLS_ROOT)/..."; \
		cargo install --locked --force --root $(TOOLS_ROOT) cargo-zigbuild --version $(CARGO_ZIGBUILD_VERSION) || exit 1; \
	fi; \
	echo "✓ cargo-zigbuild $(CARGO_ZIGBUILD_VERSION) at $(CARGO_ZIGBUILD)"

# rustup keeps targets per toolchain, so these cannot live in the checkout;
# they are added to the pinned toolchain whenever one is missing.
ensure-dist-targets: ensure-cargo
	@MISSING=$$(INSTALLED=$$(rustup target list --installed); \
		for t in $(DIST_TARGETS); do echo "$$INSTALLED" | grep -qx $$t || echo $$t; done); \
	if [ -n "$$MISSING" ]; then rustup target add $$MISSING || exit 1; fi; \
	echo "✓ Rust targets for $$(rustc --version | awk '{print $$2}'): $(DIST_TARGETS)"


# A double-colon rule: a file included below may add its own `help::` lines.
.PHONY: help
help::
	@echo "DelightQL Dependency Management"
	@echo ""
	@echo "Targets:"
	@echo "  make [build]           - Check cargo+uv, build dql -> target/debug/dql"
	@echo "  make ship              - Optimized build (fat LTO, stripped) -> target/release-ship/dql"
	@echo "  make dist              - Release tarballs for every platform this host can build -> dist/"
	@echo "  make dist-setup        - Install the pinned zig + cargo-zigbuild into $(TOOLS_ROOT)/ and add the Rust targets (dist does this too)"
	@echo "  make dist-linux        - Only the three Linux tarballs (x86_64/aarch64 musl, aarch64 glibc)"
	@echo "  make dist-macos        - Only the macOS universal tarball (skipped off macOS)"
	@echo "  make setup             - Ensure all build dependencies are installed"
	@echo "  make ensure-tree-sitter - Install the pinned tree-sitter CLI $(TREE_SITTER_EXPECTED_VERSION) into $(TOOLS_ROOT)/"
	@echo "  make generate-grammar  - Generate the parser from grammar.js (derived, ignored)"
	@echo "  make help              - Show this help"
	@echo ""
	@echo "Individual dependency checks:"
	@echo "  make ensure-rust       - Check Rust"
	@echo "  make ensure-duckdb     - Check libduckdb (only the duckdb fatboy links it)"
	@echo "  make ensure-node       - Check Node.js (the node binding)"

# Optional: absent in a checkout that does not carry it.
-include private.mk
