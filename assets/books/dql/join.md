# Join {.dqlh}

Joins extend a relation's schema by combining columns from multiple sources.
Every join is a filtered cross product -- the join condition determines which
pairings survive.

Delightql evaluates joins left to right. Each table must be in scope before its
columns can be referenced.

## Cross Join {.dqlh}

```delightql
employee(*), department(*)
```

```sql
SELECT * FROM employee CROSS JOIN department;
```

Two relations joined with no condition produce a Cartesian product. The
resulting cardinality is the product of both input cardinalities. This is
rarely intended.

## Inner Join {.dqlh}

```delightql
employee(*), department(*), employee.DepartmentId = department.DepartmentId
```

```sql
SELECT * FROM employee
  JOIN department ON employee.DepartmentId = department.DepartmentId;
```

The join condition follows the tables it correlates. Multiple conditions conjoin naturally:

```delightql
employee(*), department(*),
  employee.DepartmentId = department.DepartmentId,
  employee.Location = department.Location
```

**Scope is left to right.** This is an error:
```{.delightql .bad}
// INVALID: department not yet in scope
employee(*), department.DepartmentId = employee.DepartmentId, department(*)
```

## `USING` Shorthand {.dqlh}

The `.(cols)` operator specifies USING columns:
```delightql
employee(*), department(*.(DepartmentId))
```
```sql
SELECT * FROM employee JOIN department USING (DepartmentId);
```

Multiple columns are comma-separated:
```delightql
employee(*), department(*.(DepartmentId, LocationId))
```
```sql
SELECT * FROM employee JOIN department USING (DepartmentId, LocationId);
```

`USING` and explicit `ON` differ operationally: `USING` retains one copy of the matched column; `ON` retains both.

`USING` may combine with explicit conditions:
```delightql
employee(*), department(*.(DepartmentId)), employee.StartDate > department.Founded
```


## Multi-Table Joins {.dqlh}

Joins chain left to right. Each table enters scope upon appearance:
```delightql
employee(*),
  department(*.(DepartmentId)),
  location(*.(LocationId)),
  employee.StartDate > "2020-01-01"
```
```sql
SELECT * FROM employee
  JOIN department USING (DepartmentId)
  JOIN location USING (LocationId)
WHERE employee.StartDate > '2020-01-01';
```

After the third table, columns from all three are in scope.

## Self-Join {.dqlh}

Aliases distinguish multiple references to the same table:
```delightql
employee(*) as e, employee(*) as mgr, e.ManagerId = mgr.Id
  |> (e.Name as Employee, mgr.Name as Manager)
```
```sql
SELECT e.Name AS Employee, mgr.Name AS Manager
FROM employee e
  JOIN employee mgr ON e.ManagerId = mgr.Id;
```

## Argumentative Join {.dqlh}

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


## Outer Joins {.dqlh}

The OUTER-IND sigil `?`{.delightql .sigil} marks a relation as optional -- it
may contribute nulls when no match exists.

**Left outer** (right table optional):
```delightql
employee(*), department?(*.(DepartmentId))
```
```sql
SELECT * FROM employee LEFT OUTER JOIN department USING (DepartmentId);
```

**Right outer** (left table optional):
```delightql
employee?(*), department(*.(DepartmentId))
```

**Full outer** (either optional):
```delightql
employee?(*), department?(*.(DepartmentId))
```

Outer joins work with explicit conditions:
```delightql
employee(*), department?(*), employee.DepartmentId = department.DepartmentId
```
```sql
SELECT * FROM employee
  LEFT OUTER JOIN department ON employee.DepartmentId = department.DepartmentId;
```

### ON vs WHERE Inference {.dqlh}

For inner joins, the placement of conditions in `ON` versus `WHERE` is
semantically equivalent. For outer joins, it differs: `ON` conditions govern
the match while preserving nulls; `WHERE` conditions filter the result and
eliminate nulls.

Delightql infers placement from column references:

- **Condition references multiple tables** → `ON`
- **Condition references one table** → `WHERE`
```delightql
employee(*), department?(*),
  employee.DepartmentId = department.DepartmentId,   -- two tables → ON
  department.Status = "active"                        -- one table → WHERE
```
```sql
SELECT * FROM employee
  LEFT OUTER JOIN department
    ON employee.DepartmentId = department.DepartmentId
WHERE department.Status = 'active';
```

The multi-table condition (`employee.DepartmentId = department.DepartmentId`) becomes the
join's `ON` clause. The single-table condition (`department.Status = 'active'`)
becomes a `WHERE` filter -- employees with null or inactive departments are
excluded.


## Semi-Join and Anti-Join {.dqlh}

Semi-joins and anti-joins test existence without adding columns -- they are
predicates, not joins. See **Where**.

## Lateral Joins {.dqlh}

Correlated subqueries that return multiple columns use interior relation syntax. See **Interior Relations**.

## ER-Context Joins {.dqlh}

When join relationships are defined via **ER-context-rules** (see DDL), the `&` and `&&` operators provide concise join syntax:
```delightql
under normal:
  users(*) & orders(*)
```

Equivalent to:
```delightql
users(*), orders(*), users.id = orders.user_id
```

The `&` operator performs direct lookup; `&&` finds a path through the ER-graph:
```delightql
under normal: users(*) && items(*)
// Compiler finds: users -> orders -> items
```

ER-context joins compose with all other features -- filters, projections, aggregations, additional explicit joins.

For defining ER-rules and contexts, see **DDL: ER-Context Rules**.
