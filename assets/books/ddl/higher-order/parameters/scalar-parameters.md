
# Scalar Parameters {.dqlh}

A bare identifier without parentheses is a scalar parameter.  It binds
a single value used directly in body expressions:

```delightql
high_earners(T(*), salary_floor, min_count)(*) :-
  T(*), Salary > salary_floor,
    department(*.(DepartmentId))
    |> %(department ~> count:(*) as employee_count),
    employee_count > min_count
```

```delightql
high_earners(employee(*), 50000, 10)(*)
```

