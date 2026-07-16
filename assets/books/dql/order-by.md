# Order By {.dqlh}


Delightql, like SQL, has order by. The ORDER-BY operator is an
octothorpe-prefixed functor `#()`{.delightql .sigil} applied after a pipe:

```delightql
employee(*)
    |>  #(FirstName,LastName)
```

```Sql
select
  *
from employee order by FirstName, LastName;
```

Columns appear in the SQL in the order given. Collation modifiers work as in
SQL--with keywords:

```delightql
employee(*)
    |>  #(Salary descending,LastName ascending)
```


```Sql
select
  *
from employee order by Salary desc, LastName asc;
```

> `Order By` has no meaning in pure relational algebra, where relations are
> unordered sets. SQL has never been true to theory; delightql is equally
> cavalier. To admit order-by as a first-class relational operator, delightql
> takes two positions:
>
>   - Relations are ordered sequences of tuples, not sets. [A **sequence** captures this notion.]{.sidenote}
>   - A parametric mechanism maps domain orderings onto tuple orderings. [We can
> imagine every tuple contains a hidden column `#`. Absent an explicit ordering,
> this column holds arbitrary values. Given `order by my_column`, the tuples
> reorder and # recalculates accordingly.]{.sidenote}
