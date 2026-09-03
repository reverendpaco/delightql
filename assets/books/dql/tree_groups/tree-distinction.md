
# Tree Distinction {.dqlh}

Tree structures can serve as grouping columns, enabling aggregation alongside
hierarchical output:
```delightql
employee(*)
  |> %( { Title,
          "people": ~> {FirstName, LastName},
          State } as people_by_title_and_state
          ~>
        sum:(Salary), count:(*) )
```


**Restriction:** Columns referenced in nested tree groups cannot also appear as
explicit grouping columns:

```{.delightql .bad}
// INVALID: LastName appears in tree group and as grouping column
employee(*)
  |> %( { Title, "people": ~> {FirstName, LastName}, State } as tree,
        LastName
          ~>
        sum:(Salary) )
```

Columns not referenced in the tree may be added:
```delightql
employee(*)
  |> %( { Title, "people": ~> {FirstName, LastName}, State } as tree,
        DepartmentId
          ~>
        sum:(Salary), count:(*) )
```

