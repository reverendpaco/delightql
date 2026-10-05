# Whole-Group Aggregate Functions {.dqlh}

To aggregate without grouping, omit the grouping columns:

```delightql
invoice(*)
  |>  %( ~>  count:(*) , sum:(total) )
```

```sql
  select
    count(*),
    sum(total)
  from invoice;
```

The GROUP-PIPE `~>`{.delightql .sigil} provides a shorter form for a single
aggregate:

```delightql
invoice(*) ~>  count:(*)
```

**Note**.  The **GROUP-PIPE** is different from the *AGG-AND*. This form
replaces the `|>`{.delightql} pipe.  It is pure sugar.
