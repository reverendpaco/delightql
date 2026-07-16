# Limit {.dqlh}

Limit the number of tuples returned using the **TUPLE-ORDINAL** sigil `#`{.delightql .sigil} in a predicate position:

```delightql
employee(*) , # < 20
```

```Sql
select * from employee limit 20;
```

Read this as: "all columns of employee where the implicit row ordinal is less
than 20."

Limit affects only cardinality, not schema.


**Order of operations matters**. Delightql evaluates left to right, so these two queries differ:

```delightql
employee(*), department(*.(DepartmentName)), #<20
```

```sql
select
  *
from employee join department using(DepartmentName)
  limit 20;
```

```delightql
employee(*), #<20, department(*.(DepartmentName))
```

```sql
select
  *
from (select * from employee limit 20)
  join department using(DepartmentName);
```
