
# Common Function Expressions {.dqlh}

Create common function expressions (**CFEs**) -- functions whose name is
created for the duration of the query -- by *pre-labeling* with a *functional
functor*. [A functional functor is a functor with a colon separating the
identifier from the opening parenthesis.]{.sidenote} The **SHADOW-NECK**
separates the functional functor on the left from any valid domain expression
on the right. CFEs may **only** be created by pre-labeling.

```delightql
enweirden:(age) :
  age /-> :(@ - 18) /-> max:(0) /-> min:(100)

users(*) |> (enweirden:(age) as silly, age)
```

```sql
SELECT
  min(max("age" - 18, 0), 100) AS "silly",
  "age" AS "age"
FROM "users";
```



CTEs and CFEs may be intermixed:

```delightql
double:(x) : (x * 2)
users(*), age > 25 : adults
triple:(y) : (y * 3)
young_adults(*): adults(*), age < 40
young_adults(*)
  |> ( id,
      first_name,
      age,
      double:(age) as doubled,
      age /-> double:() /-> double:() as quadrupled,
      triple:(age) as tripled,
      double:(triple:(age)) as sextupled)
```

```sql
WITH adults AS (
    SELECT
        *
    FROM users
    WHERE age > 25
),
young_adults AS (
    SELECT
        *
    FROM adults
    WHERE age < 40
)
SELECT
    id,
    first_name,
    age,
    (age * 2) AS doubled,
    ((age * 2) * 2) AS quadrupled,
    (age * 3) AS tripled,
    ((age * 3) * 2) AS sextupled
FROM
    young_adults;
```

