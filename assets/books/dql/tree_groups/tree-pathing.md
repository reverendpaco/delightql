
# Pathing in Tree Patterns {.dqlh}

Destructuring patterns support direct pathing, eliminating the need to match
intermediate structure. The pathing syntax (`.path.to.field`) reaches into
nested JSON without declaring every level.

**Basic pathing:**
```delightql
_(json @ {"name": "app", "config": {"server": {"port": 3000}}})
  |> (json:{.config.server.port})
```

The path `.config.server.port` extracts the value directly.

**Pathing in destructuring:**

Instead of matching the full structure:
```delightql
partner_sale(*), payload ~= { partner_order, "buyer": { email, country } }
```

Path directly to what you need:
```delightql
partner_sale(*), payload ~= {
  partner_order,
  .buyer.email,
  .buyer.country
}
```

**Pathing with rename:**

Combine pathing with `as` to name the output column:
```delightql
partner_sale(*) |> (sale_id, payload:{.items} as items)
  , items ~= ~> {
      track_id,
      .rights.territories as territories
    }
```

**Mixed matching and pathing:**

Structural matching and pathing can combine in a single pattern:
```delightql
partner_sale(*), payload ~= {
  partner_order,
  payout,
  .buyer.email,
  .buyer.country
}
```

Here `partner_order` and `payout` match top-level keys directly; the `.buyer.*`
paths reach into nested structure.

**Pathing in projection:**

Pathing works outside destructuring patterns, in normal projection:
```delightql
_(json @ {"name": "app", "scripts": {"dev": "next dev", "build": "next build"}})
  |> ({
    "name": json:{.name},
    "scripts": json:{.scripts}
  })
```


