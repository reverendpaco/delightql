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

.PHONY: bundle books man clean
bundle: books man

books:
	@mkdir -p bundled
	uv run bin/bundle.py books --content books --output bundled/books.sqlite --content-version $(CONTENT_VERSION)

man:
	@mkdir -p bundled
	uv run bin/bundle.py man --man-dir man/man1 --output bundled/man.sqlite --content-version $(CONTENT_VERSION)

clean:
	rm -rf bundled
