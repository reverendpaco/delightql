# Whole-Group Aggregate Functions {.dqlh}

To aggregate without grouping, omit the grouping columns:

```delightql
employee(*)
  |>  %( ~>  count:(*) , sum:(Salary) )
```

```sql
  select
    count(*),
    sum(Salary)
  from employee;
```

The GROUP-PIPE `~>`{.delightql .sigil} provides a shorter form for a single
aggregate:

```delightql
employee(*) ~>  count:(*)
```

**Note**.  The **GROUP-PIPE** is different from the *AGG-AND*. This form
replaces the `|>`{.delightql} pipe.  It is pure sugar.
