
# Higher-Order Rules {.dqlh}

Higher-order rules are rules that accept tables or scalars as parameters -- inputs that further instantiate a templated relation. SQL calls these table-valued functions; Prolog would call them higher-order predicates.

All higher-order rules return a table value. It is this specific quality that permits us calling this abstraction "higher-order" and not the fact that the inputs may be tables themselves.

A programmer can create their own higher-order rules and use them in all places where a table is allowed.  With a certain kind of definition -- one in which the final parameter is itself a table -- a higher-order rule may also be used after a pipe.

```delightql
employee_2019(*)
  |> clean_employees(*)
```

Higher-order rules are also called **input-moded rules**.
