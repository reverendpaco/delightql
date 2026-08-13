# The assets front door (BOOK-NEXT-GEN.md section 7). build.rs invokes
# this once; both embedded databases come through here and build.rs
# synthesizes nothing.
#
# Targets are PHONY on purpose: mtime prerequisites are blind to
# DELETIONS (a removed source vanishes from the prerequisite list, so a
# stale database still looks current). The bundler is deterministic and
# sub-second — the bundler IS the fingerprint. It rewrites its output
# only when the bytes differ, so downstream mtime consumers stay quiet.

CONTENT_VERSION := $(shell sed -n 's/^version = "\(.*\)"/\1/p' ../Cargo.toml | head -1)

# The platform triple recorded in the editor bundle's parser_artifact.
# build.rs passes cargo's TARGET explicitly; manual runs fall back to the
# rustc host triple (the same value for any non-cross build).
TARGET ?= $(shell rustc -vV | sed -n 's/^host: //p')

GRAMMAR := ../grammar

.PHONY: bundle books man editor clean
bundle: books man editor

books:
	@mkdir -p bundled
	uv run bin/bundle.py books --content books --output bundled/books.sqlite --content-version $(CONTENT_VERSION)

man:
	@mkdir -p bundled
	uv run bin/bundle.py man --man-dir man/man1 --output bundled/man.sqlite --content-version $(CONTENT_VERSION)

editor: $(GRAMMAR)/delightql.so
	@test -n "$(TARGET)" || { echo "TARGET is required (platform triple for parser_artifact)"; exit 1; }
	@mkdir -p bundled
	uv run bin/bundle.py editor --grammar-dir $(GRAMMAR) --queries-dir $(GRAMMAR)/queries \
		--parser-so $(GRAMMAR)/delightql.so --parser-src $(GRAMMAR)/src/parser.c \
		--target $(TARGET) --output bundled/editor.sqlite --content-version $(CONTENT_VERSION)

# parser.c has ONE producer: delightql-cst's build.rs, which enforces the
# pinned tree-sitter CLI. No generate rule here — a second invocation
# would run whatever CLI is on PATH, unpinned. delightql-cli depends on
# delightql-cst, so by the time build.rs opens this front door the
# parser is current; a missing parser.c fails loudly ("no rule to make
# target" — run cargo build first).
# Compilation is the expensive step (seconds, not sub-second), so unlike
# the bundling targets it carries a real mtime prerequisite.
$(GRAMMAR)/delightql.so: $(GRAMMAR)/src/parser.c
	cc -o $@ -I$(GRAMMAR)/src $(GRAMMAR)/src/parser.c -shared -Os -fPIC

clean:
	rm -rf bundled
