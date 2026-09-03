# Argumentative Grounding {.dqlh}

When using argumentative functor notation, a ground term in argument position induces selection:

```delightql
stock_ownership(1,stock_id,stock_name,quantity)
```

```sql
select
  stock_id,
  stock_name,
  quantity
from stock_ownership where people_id IS NOT DISTINCT FROM 1;
```

**All argumentative grounding uses null-safe equality**.

The grounded column (`people_id  IS NOT DISTINCT FROM  1`) filters rows and is excluded from projection. Multiple grounds compound:

```delightql
stock_ownership(people_id,5,stock_name,120)
```


```sql
SELECT people_id, stock_name
FROM stock_ownership
WHERE
  stock_id IS NOT DISTINCT FROM 5
  AND quantity IS NOT DISTINCT FROM 120;
```

Any domain expression that reduces to a ground term may also be used in argumentative position:

```delightql
stock_ownership(people_id,(4 + 1),upper:("msft"),120)
```

```sql
select
  people_id
from stock_ownership where stock_id IS NOT DISTINCT FROM (4+1) and quantity IS NOT DISTINCT FROM 120 and stock_name IS NOT DISTINCT FROM upper('msft');
```

The Prolog heritage is evident in this syntax and extends to joins -- covered in a later section.
