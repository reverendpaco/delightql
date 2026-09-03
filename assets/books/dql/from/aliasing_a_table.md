# Aliasing a Table {.dqlh}

```delightql
employee(*) as e
```

The `as` keyword assigns an alias to a table. Once aliased, columns must be
accessed through the alias -- the original table name leaves scope.

```sql
select * from employee as e;
```

Aliases are often a convenience,
but become necessary in contexts like self-joins.
