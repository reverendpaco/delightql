-- Seed rows: dialect_form_rule (ALL-SQL-TARGETING-DESIGN.md §4.1).
-- Executed at the END of sync_bin_cartridges_to_bootstrap — per-functor
-- rules reference entity rows by id, and entity ids are session-local
-- insertion order, so the seed resolves them by (name, type) subselect.
-- Idempotent via NOT EXISTS (sync runs on init AND reinit).
--
-- +like → ILIKE on postgres/duckdb: a FIDELITY rule, not a spelling
-- preference. Canonical (SQLite) LIKE is case-insensitive for ASCII;
-- postgres and duckdb LIKE are case-sensitive, so canonical spelling
-- silently changes match semantics there. ILIKE restores them. The
-- template expresses the un-negated predicate; the generator wraps NOT(...)
-- for `\+like`.
--
-- DISCIPLINE: a fidelity rule creates a language-semantic commitment
-- ("+like is case-insensitive everywhere" is now a fact about DelightQL).
-- Every fidelity rule added here owes a matching statement in the book
-- where users read semantics (+like: book/reference/appendices/stdlib.md),
-- or the semantics drift into data nobody reads.

INSERT INTO dialect_form_rule (form_type, dialect, entity_id, rule_kind, body)
SELECT
    (SELECT id FROM entity_type_enum WHERE variant = 'BinSigmaPredicate'),
    d.dialect,
    e.id,
    'template',
    '{0} ILIKE {1}'
FROM entity e
JOIN (SELECT 'postgres' AS dialect UNION ALL SELECT 'duckdb') d
WHERE e.name = 'like'
  AND e.type = (SELECT id FROM entity_type_enum WHERE variant = 'BinSigmaPredicate')
  AND NOT EXISTS (
      SELECT 1 FROM dialect_form_rule f
      WHERE f.dialect = d.dialect AND f.entity_id = e.id
  );
