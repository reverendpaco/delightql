
# Argumentative Join {.dqlh}

Shared identifiers across functors induce join conditions -- Prolog-style unification:
```delightql
employee(Name, Department), department(Department, location)
```
```sql
SELECT employee.Name, employee.Department, department.location
FROM employee
  JOIN department ON employee.Department = department.Department;
```

The variable `Department` appears in both functors, unifying the columns.

Multi-table example:
```delightql
people(people_id, _, last_name),
  stock_ownership(people_id, stock_id, quantity),
  stocks(stock_id, stock_name),
  quantity < 200
  |> (last_name, stock_name)
```

Argumentative joins are idiomatic in Prolog. Delightql supports them but
recommends `.(cols)` or explicit conditions for wide tables where positional
notation becomes error-prone.


