
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
j ~= { name, "config": { "server": { port, host }, "database": { url } } }
```

Path directly to what you need:
```delightql
j ~= {
  name,
  .config.server.port,
  .config.server.host,
  .config.database.url
}
```

**Pathing with rename:**

Combine pathing with `as` to name the output column:
```delightql
user_data ~= ~> {
  country,
  .name_info.last_name as ln,
  .name_info.first_name as fn
}
```

**Mixed matching and pathing:**

Structural matching and pathing can combine in a single pattern:
```delightql
j ~= {
  name,
  version,
  .dependencies.react,
  .dependencies.next
}
```

Here `name` and `version` match top-level keys directly; the `.dependencies.*`
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


