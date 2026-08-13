#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Daniel Eklund
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml>=6"]
# ///
"""asset-bundler: authored sources -> compliant embedded databases.

The one producer behind the assets/ front door (BOOK-NEXT-GEN.md):

  bundle.py books --content books --output bundled/books.sqlite
      markdown atoms + yml spines -> the DQLB book bundle (section 6)
  bundle.py man --man-dir man/man1 --output bundled/man.sqlite
      troff pages -> the DQLM man bundle
  bundle.py editor --grammar-dir ../grammar ... --output bundled/editor.sqlite
      authored grammar + editor queries + compiled parser -> the DQLE
      editor-support bundle

It refuses what breaks the book's links or the databases' shape; it never
judges the prose. build.rs re-verifies compliance at the embedding seam —
the bundler is trusted for nothing.

Deterministic by construction: sorted walks, no timestamps, stable
digests. Identical inputs produce byte-identical output (for a given
sqlite library version), and an unchanged output file is left untouched.
"""

import argparse
import hashlib
import os
import re
import sqlite3
import sys

import yaml

BOOK_APPLICATION_ID = 0x4451_4C42  # "DQLB", mirrored in cli embedded_db.rs
MAN_APPLICATION_ID = 0x4451_4C4D  # "DQLM", mirrored in cli embedded_db.rs
EDITOR_APPLICATION_ID = 0x4451_4C45  # "DQLE", mirrored in cli embedded_db.rs
SCHEMA_VERSION = 1

BOOK_SCHEMA_SQL = f"""
PRAGMA application_id = {BOOK_APPLICATION_ID};
PRAGMA user_version = {SCHEMA_VERSION};
PRAGMA foreign_keys = ON;
CREATE TABLE bundle_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL,
    content_version TEXT NOT NULL,
    source_digest TEXT NOT NULL
);
CREATE TABLE base_content (
    slug TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    source_path TEXT NOT NULL,
    content_digest TEXT NOT NULL
);
CREATE TABLE book (
    book_name TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    heading_shift INTEGER NOT NULL DEFAULT 0 CHECK (heading_shift >= 0),
    slug TEXT NOT NULL,
    PRIMARY KEY (book_name, ordinal),
    FOREIGN KEY (slug) REFERENCES base_content(slug)
);
CREATE TABLE book_meta (
    book_name TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    frontmatter TEXT
);
CREATE TABLE image (
    name TEXT PRIMARY KEY,
    media_type TEXT NOT NULL,
    content BLOB NOT NULL,
    content_digest TEXT NOT NULL
);
"""

MAN_SCHEMA_SQL = f"""
PRAGMA application_id = {MAN_APPLICATION_ID};
PRAGMA user_version = {SCHEMA_VERSION};
CREATE TABLE bundle_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL,
    content_version TEXT NOT NULL,
    source_digest TEXT NOT NULL
);
CREATE TABLE man_page (
    name TEXT NOT NULL,
    section INTEGER NOT NULL,
    troff TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    PRIMARY KEY (name, section)
);
"""

EDITOR_SCHEMA_SQL = f"""
PRAGMA application_id = {EDITOR_APPLICATION_ID};
PRAGMA user_version = {SCHEMA_VERSION};
CREATE TABLE bundle_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL,
    content_version TEXT NOT NULL,
    source_digest TEXT NOT NULL
);
CREATE TABLE grammar_source (
    path TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    content_digest TEXT NOT NULL
);
CREATE TABLE editor_query (
    name TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    content_digest TEXT NOT NULL
);
CREATE TABLE parser_artifact (
    target TEXT PRIMARY KEY,
    abi_version INTEGER NOT NULL CHECK (abi_version >= 1),
    content BLOB NOT NULL,
    content_digest TEXT NOT NULL
);
"""


class BundleError(Exception):
    """A contract violation, already formatted as file:line: message."""


def err(path, line, message):
    where = f"{path}:{line}" if line is not None else str(path)
    return BundleError(f"{where}: {message}")


# The bundler never reads the prose. Atoms are stored verbatim; how they
# are written (h1 roots, {.dqlh} markers) is the AUTHOR's contract,
# checked by eyeballs on the emitted book — see BOOK-NEXT-GEN.md §3. The
# bundler refuses only what breaks the book's links or the database's
# shape: spine slugs that resolve to nothing, yml that does not parse,
# duplicate book names.

# ── The yml book contract (BOOK-NEXT-GEN.md section 4) ──────────────────


def scalar(node, path, what):
    if not isinstance(node, yaml.ScalarNode) or node.tag.endswith(":null"):
        raise err(path, node.start_mark.line + 1, f"{what} must be a plain scalar")
    return node.value


def entry_slug(node, path):
    """A spine entry is the atom's FILE path relative to books/, .md
    included — so an editor can jump to it (vim gf). The slug is the
    path minus the extension."""
    value = scalar(node, path, "spine entry")
    if not value.endswith(".md"):
        raise err(
            path,
            node.start_mark.line + 1,
            f"spine entry '{value}' must name the atom file (end in .md)",
        )
    return value[: -len(".md")]


def walk_spine(node, depth, out, path):
    """Preorder walk: nesting IS shifting. Appends (slug, depth, line)."""
    if not isinstance(node, yaml.SequenceNode):
        raise err(path, node.start_mark.line + 1, "spine level must be a list")
    for entry in node.value:
        if isinstance(entry, yaml.ScalarNode):
            out.append((entry_slug(entry, path), depth, entry.start_mark.line + 1))
        elif isinstance(entry, yaml.MappingNode):
            if len(entry.value) != 1:
                raise err(
                    path,
                    entry.start_mark.line + 1,
                    "spine entry must be a file or ONE file with children "
                    "(a multi-key mapping is usually a missing '-')",
                )
            key, children = entry.value[0]
            out.append((entry_slug(key, path), depth, key.start_mark.line + 1))
            walk_spine(children, depth + 1, out, path)
        else:
            raise err(
                path,
                entry.start_mark.line + 1,
                "spine entry must be a file or a file with children",
            )


def parse_book(path, text):
    try:
        root = yaml.compose(text)
    except yaml.YAMLError as e:
        raise err(path, None, f"yml does not parse: {e}")
    if not isinstance(root, yaml.MappingNode):
        raise err(path, 1, "book file must be a mapping")
    fields = {}
    for key, value in root.value:
        name = scalar(key, path, "book field name")
        if name in fields:
            raise err(path, key.start_mark.line + 1, f"duplicate field '{name}'")
        fields[name] = value
    allowed = {"book", "title", "frontmatter", "spine"}
    for name in fields:
        if name not in allowed:
            raise err(
                path,
                fields[name].start_mark.line + 1,
                f"unknown field '{name}' (allowed: {', '.join(sorted(allowed))})",
            )
    for required in ("book", "title", "spine"):
        if required not in fields:
            raise err(path, None, f"missing required field '{required}'")
    placements = []
    walk_spine(fields["spine"], 0, placements, path)
    if not placements:
        raise err(path, None, "spine is empty")
    return {
        "book": scalar(fields["book"], path, "book"),
        "title": scalar(fields["title"], path, "title"),
        "frontmatter": scalar(fields["frontmatter"], path, "frontmatter")
        if "frontmatter" in fields
        else None,
        "placements": placements,
    }


# ── Digests (length-prefixed fields, same scheme as build.rs) ───────────


def digest_field(hasher, data):
    hasher.update(len(data).to_bytes(8, "big"))
    hasher.update(data)


# Images are pool citizens (BOOK-NEXT-GEN.md section 10): ingested
# unconditionally from anywhere under books/, identified by BASENAME,
# content-addressed. No reference checking — that would mean reading the
# prose; a dangling image ref fails visibly at press time when pandoc
# names the missing file. The one refusal is pool identity: the same
# basename with different bytes.
IMAGE_TYPES = {
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
}


# ── Main ────────────────────────────────────────────────────────────────


def collect(content_dir):
    atoms = {}  # slug -> dict
    books = []
    all_inputs = []  # (relative_path, text) for source_digest
    yml_files = sorted(
        f
        for f in os.listdir(content_dir)
        if f.endswith((".yml", ".yaml")) and os.path.isfile(os.path.join(content_dir, f))
    )
    md_files = []
    image_files = []
    for root, dirs, files in os.walk(content_dir):
        dirs.sort()
        for f in sorted(files):
            if f.endswith(".md"):
                md_files.append(os.path.join(root, f))
            elif os.path.splitext(f)[1].lower() in IMAGE_TYPES:
                image_files.append(os.path.join(root, f))

    for md in md_files:
        rel = os.path.relpath(md, content_dir)
        with open(md, encoding="utf-8") as fh:
            text = fh.read()
        slug = rel[: -len(".md")]
        atoms[slug] = {
            "slug": slug,
            "title": os.path.splitext(os.path.basename(md))[0],
            "content": text,
            "source_path": rel,
            "digest": hashlib.sha256(text.encode()).hexdigest(),
        }
        all_inputs.append((rel, text))

    images = {}  # basename -> dict
    for img in image_files:
        rel = os.path.relpath(img, content_dir)
        name = os.path.basename(img)
        with open(img, "rb") as fh:
            data = fh.read()
        digest = hashlib.sha256(data).hexdigest()
        existing = images.get(name)
        if existing is not None:
            if existing["digest"] != digest:
                raise err(
                    rel,
                    None,
                    f"image '{name}' already in the pool with different "
                    f"bytes (from {existing['source_path']})",
                )
            continue  # identical bytes elsewhere: one row, no drift
        images[name] = {
            "name": name,
            "media_type": IMAGE_TYPES[os.path.splitext(name)[1].lower()],
            "content": data,
            "digest": digest,
            "source_path": rel,
        }
        all_inputs.append((rel, data))

    seen_names = {}
    for yml in yml_files:
        full = os.path.join(content_dir, yml)
        with open(full, encoding="utf-8") as fh:
            text = fh.read()
        book = parse_book(full, text)
        if book["book"] in seen_names:
            raise err(full, None, f"book '{book['book']}' already defined in {seen_names[book['book']]}")
        seen_names[book["book"]] = yml
        for slug, _depth, line in book["placements"]:
            if slug not in atoms:
                raise err(full, line, f"spine slug '{slug}' has no atom in the pool")
        books.append(book)
        all_inputs.append((yml, text))

    logical = hashlib.sha256()
    for rel, data in sorted(all_inputs, key=lambda pair: pair[0]):
        digest_field(logical, rel.encode())
        digest_field(logical, data.encode() if isinstance(data, str) else data)
    return atoms, images, books, logical.hexdigest()


def build_database(output, populate):
    """Build into a temp file via `populate(conn)`, integrity-check, and
    publish. Deterministic output makes "did anything change" a byte
    comparison: an identical database is left untouched so downstream
    mtime consumers stay quiet (the Makefile always runs us; this is the
    staleness check). Returns True when the output file changed."""
    temp = output + ".tmp"
    if os.path.exists(temp):
        os.remove(temp)
    conn = sqlite3.connect(temp)
    try:
        populate(conn)
        conn.commit()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise BundleError(f"emitted database failed integrity_check: {integrity}")
    finally:
        conn.close()
    if os.path.exists(output):
        with open(temp, "rb") as a, open(output, "rb") as b:
            if a.read() == b.read():
                os.remove(temp)
                return False
    os.replace(temp, output)
    return True


def emit_books(output, atoms, images, books, source_digest, content_version):
    def populate(conn):
        conn.executescript(BOOK_SCHEMA_SQL)
        conn.execute(
            "INSERT INTO bundle_meta VALUES (1, ?, ?, ?)",
            (SCHEMA_VERSION, content_version, source_digest),
        )
        for slug in sorted(atoms):
            a = atoms[slug]
            conn.execute(
                "INSERT INTO base_content VALUES (?, ?, ?, ?, ?)",
                (a["slug"], a["title"], a["content"], a["source_path"], a["digest"]),
            )
        for name in sorted(images):
            i = images[name]
            conn.execute(
                "INSERT INTO image VALUES (?, ?, ?, ?)",
                (i["name"], i["media_type"], i["content"], i["digest"]),
            )
        for book in sorted(books, key=lambda b: b["book"]):
            conn.execute(
                "INSERT INTO book_meta VALUES (?, ?, ?)",
                (book["book"], book["title"], book["frontmatter"]),
            )
            for ordinal, (slug, depth, _line) in enumerate(book["placements"], start=1):
                conn.execute(
                    "INSERT INTO book VALUES (?, ?, ?, ?)",
                    (book["book"], ordinal, depth, slug),
                )

    return build_database(output, populate)


# ── Man pages ────────────────────────────────────────────────────────────


def collect_man(man_dir):
    """One row per troff page; identity = <name>.<section> filename."""
    try:
        entries = sorted(os.listdir(man_dir))
    except OSError as e:
        raise BundleError(f"{man_dir}: {e}")
    pages = []
    logical = hashlib.sha256()
    for f in entries:
        full = os.path.join(man_dir, f)
        if not os.path.isfile(full) or "." not in f:
            continue
        name, _, section = f.rpartition(".")
        if not section.isdigit():
            continue
        with open(full, encoding="utf-8") as fh:
            troff = fh.read()
        digest_field(logical, name.encode())
        digest_field(logical, section.encode())
        digest_field(logical, troff.encode())
        pages.append((name, int(section), troff))
    if not pages:
        raise BundleError(f"{man_dir}: no man pages found")
    return pages, logical.hexdigest()


# ── Editor support (grammar + queries + compiled parser) ─────────────────

# The compiled parser must be a shared library for SOME platform; which
# platform is recorded in parser_artifact.target and re-verified against
# the cargo TARGET at the embedding seam.
SHARED_LIB_MAGICS = (
    b"\x7fELF",  # ELF
    b"\xcf\xfa\xed\xfe",  # Mach-O 64-bit
    b"\xca\xfe\xba\xbe",  # Mach-O universal
    b"MZ",  # PE/COFF
)


def parser_abi_version(parser_src):
    """The tree-sitter ABI the artifact speaks, read from the generated
    parser's LANGUAGE_VERSION define — the same source the .so was
    compiled from. Editors check this before dlopen."""
    try:
        with open(parser_src, encoding="utf-8", errors="replace") as fh:
            head = fh.read(8192)
    except OSError as e:
        raise BundleError(f"{parser_src}: {e}")
    m = re.search(r"^#define LANGUAGE_VERSION (\d+)$", head, re.MULTILINE)
    if m is None:
        raise err(parser_src, None, "no '#define LANGUAGE_VERSION' in header region")
    return int(m.group(1))


def collect_editor(grammar_dir, queries_dir, parser_so, parser_src, target):
    """Three kinds of content, three identities: authored grammar files by
    path, editor queries by convention name, the compiled parser by
    platform target."""
    logical = hashlib.sha256()

    try:
        top = sorted(os.listdir(grammar_dir))
    except OSError as e:
        raise BundleError(f"{grammar_dir}: {e}")
    sources = []
    for f in top:
        full = os.path.join(grammar_dir, f)
        if not os.path.isfile(full) or not f.endswith((".js", ".json")):
            continue
        with open(full, encoding="utf-8") as fh:
            text = fh.read()
        digest_field(logical, f.encode())
        digest_field(logical, text.encode())
        sources.append((f, text))
    if "grammar.js" not in (path for path, _ in sources):
        raise BundleError(f"{grammar_dir}: no grammar.js (not a tree-sitter grammar dir)")

    try:
        query_files = sorted(os.listdir(queries_dir))
    except OSError as e:
        raise BundleError(f"{queries_dir}: {e}")
    queries = []
    for f in query_files:
        full = os.path.join(queries_dir, f)
        if not os.path.isfile(full) or not f.endswith(".scm"):
            continue
        name = f[: -len(".scm")]
        with open(full, encoding="utf-8") as fh:
            text = fh.read()
        digest_field(logical, name.encode())
        digest_field(logical, text.encode())
        queries.append((name, text))
    if not queries:
        raise BundleError(f"{queries_dir}: no .scm query files found")

    try:
        with open(parser_so, "rb") as fh:
            so_bytes = fh.read()
    except OSError as e:
        raise BundleError(f"{parser_so}: {e}")
    if not so_bytes.startswith(SHARED_LIB_MAGICS):
        raise err(parser_so, None, "not a shared library (unrecognized magic bytes)")
    abi = parser_abi_version(parser_src)
    digest_field(logical, target.encode())
    digest_field(logical, str(abi).encode())
    digest_field(logical, so_bytes)
    artifact = (target, abi, so_bytes)

    return sources, queries, artifact, logical.hexdigest()


def emit_editor(output, sources, queries, artifact, source_digest, content_version):
    def populate(conn):
        conn.executescript(EDITOR_SCHEMA_SQL)
        conn.execute(
            "INSERT INTO bundle_meta VALUES (1, ?, ?, ?)",
            (SCHEMA_VERSION, content_version, source_digest),
        )
        for path, text in sources:
            conn.execute(
                "INSERT INTO grammar_source VALUES (?, ?, ?)",
                (path, text, hashlib.sha256(text.encode()).hexdigest()),
            )
        for name, text in queries:
            conn.execute(
                "INSERT INTO editor_query VALUES (?, ?, ?)",
                (name, text, hashlib.sha256(text.encode()).hexdigest()),
            )
        target, abi, so_bytes = artifact
        conn.execute(
            "INSERT INTO parser_artifact VALUES (?, ?, ?, ?)",
            (target, abi, so_bytes, hashlib.sha256(so_bytes).hexdigest()),
        )

    return build_database(output, populate)


def emit_man(output, pages, source_digest, content_version):
    def populate(conn):
        conn.executescript(MAN_SCHEMA_SQL)
        conn.execute(
            "INSERT INTO bundle_meta VALUES (1, ?, ?, ?)",
            (SCHEMA_VERSION, content_version, source_digest),
        )
        for name, section, troff in pages:
            conn.execute(
                "INSERT INTO man_page VALUES (?, ?, ?, ?)",
                (name, section, troff, hashlib.sha256(troff.encode()).hexdigest()),
            )

    return build_database(output, populate)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    books_cmd = sub.add_parser("books", help="bundle the book pool + spines")
    books_cmd.add_argument("--content", required=True, help="books/ directory")
    man_cmd = sub.add_parser("man", help="bundle the troff man pages")
    man_cmd.add_argument("--man-dir", required=True, help="man/man1 directory")
    editor_cmd = sub.add_parser(
        "editor", help="bundle grammar + editor queries + compiled parser"
    )
    editor_cmd.add_argument(
        "--grammar-dir", required=True, help="authored tree-sitter grammar directory"
    )
    editor_cmd.add_argument(
        "--queries-dir", required=True, help="directory of .scm editor queries"
    )
    editor_cmd.add_argument(
        "--parser-so", required=True, help="compiled parser shared library"
    )
    editor_cmd.add_argument(
        "--parser-src", required=True, help="generated parser.c (ABI version source)"
    )
    editor_cmd.add_argument(
        "--target", required=True, help="platform triple the parser was compiled for"
    )
    for p in (books_cmd, man_cmd, editor_cmd):
        p.add_argument("--output", required=True, help="sqlite file to emit")
        p.add_argument(
            "--content-version",
            default="dev",
            help="informational version recorded in bundle_meta",
        )
    args = parser.parse_args()
    try:
        if args.command == "books":
            atoms, images, books, source_digest = collect(args.content)
            if not atoms:
                raise BundleError(f"{args.content}: no markdown atoms found")
            changed = emit_books(
                args.output, atoms, images, books, source_digest, args.content_version
            )
            total = sum(len(b["placements"]) for b in books)
            summary = (
                f"bundled {len(atoms)} atoms, {len(images)} images, "
                f"{len(books)} books ({total} placements)"
            )
        elif args.command == "editor":
            sources, queries, artifact, source_digest = collect_editor(
                args.grammar_dir,
                args.queries_dir,
                args.parser_so,
                args.parser_src,
                args.target,
            )
            changed = emit_editor(
                args.output, sources, queries, artifact, source_digest, args.content_version
            )
            summary = (
                f"bundled {len(sources)} grammar files, {len(queries)} queries, "
                f"parser for {artifact[0]} (abi {artifact[1]})"
            )
        else:
            pages, source_digest = collect_man(args.man_dir)
            changed = emit_man(args.output, pages, source_digest, args.content_version)
            summary = f"bundled {len(pages)} man pages"
    except BundleError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"{summary} -> {args.output}{'' if changed else ' (unchanged)'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
