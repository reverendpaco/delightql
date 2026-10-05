# Having {.dqlh}

Filter on reduced columns by placing a predicate after the `group by`:

```delightql
customer(*)
  |> %( country ~> count:(*) as customer_count)
      ,  customer_count > 4
```

Read this as: "group customers by country, count each group, then keep only
groups with more than 4 rows."


```sql
select
  country,
  count(*) as customer_count
from customer
  group by country
    having count(*) > 4;
```



> **Why does SQL have both WHERE and HAVING?**
>
> SQL has an implicit order of operations. `WHERE` filters rows before grouping;
> `HAVING` filters groups after aggregation. The two keywords signal this
> distinction. [For a historical reflection on this issue, see
> `tpt:#fc(<HAVINGBlunderfulTime>)`.]{.sidenote}
>
>
> The abstraction is leaky -- most programmers soon recognize that `HAVING` is
> equivalent to wrapping in a subquery and filtering with `WHERE`:
>
> ```sql
> SELECT country, count(*) AS customer_count
> FROM customer
> GROUP BY country
> HAVING count(*) > 4;
>
>
> -- equivalent to:
>
> SELECT * FROM (
> SELECT country, count(*) AS customer_count
> FROM customer
> GROUP BY country
> ) WHERE customer_count > 4;
> ```
>
> Because delightql has explicit order of operations, no separate syntax is
> needed. The predicate simply follows the group by:
>
> ```delightql
> customer(*)
> |> %(country ~> count:(*) as customer_count),
> customer_count > 4
> ```
>
> Placing the filter earlier would be an error, `customer_count` does not exist until after the aggregation.
