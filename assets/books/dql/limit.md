# Limit {.dqlh}

Limit the number of tuples returned using the **TUPLE-ORDINAL** sigil `#`{.delightql .sigil} in a predicate position:

```delightql
customer(*) , # < 20
```

```sql
select * from customer limit 20;
```

Read this as: "all columns of customer where the implicit row ordinal is less
than 20."

Limit affects only cardinality, not schema.


**Order of operations matters**. Delightql evaluates left to right, so these two queries differ:

```delightql
customer(*), invoice(*.(customer_id)), #<20
```

```sql
select
  *
from customer join invoice using(customer_id)
  limit 20;
```

```delightql
customer(*), #<20, invoice(*.(customer_id))
```

```sql
select
  *
from (select * from customer limit 20)
  join invoice using(customer_id);
```
