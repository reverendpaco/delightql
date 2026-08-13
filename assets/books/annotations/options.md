# Config Annotations {.dqlh}

Where danger gates control *safety* (off by default, opened to permit
risky behavior), config annotations control *preferences* -- which code path
the compiler uses when multiple paths lead to the same result. A query with a config annotation produces
the same logical result regardless of the config state; only the
implementation strategy may differ.

Configs may be used for non-query reasons too.

## Syntax {.dqlh}

A config annotation is a `config://` hierarchy inside annotation delimiters:

```delightql
users(*) (~~config://generation/rule/inlining/view ON~~) |> (id, first_name)
```

The toggle values are identical to danger gates:

| Toggle | Meaning |
|--------|---------|
| `ON` | Enable the strategy for this query |
| `OFF` | Disable the strategy (restore default) |
| `1`--`9` | Graduated preference levels |

## Scoping {.dqlh}

Like danger gates, config annotations are scoped to a single query and
auto-revert at query end. Multiple config annotations may appear on the
same query.

## Session Baseline {.dqlh}

The CLI can shift the baseline for a session:

```bash
dql query --config generation/rule/inlining/view=ON --db test.db "..."
```

Per-query annotations override the session baseline.

## Known Options {.dqlh}

| URI | Default | What it controls |
|-----|---------|------------------|
| `generation/rule/inlining/view` | OFF | View inlining strategy during SQL generation |
| `generation/rule/inlining/fact` | OFF | Fact inlining strategy during SQL generation |
