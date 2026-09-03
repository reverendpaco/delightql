
# Definition Syntax {.dqlh}

A higher-order rule has two sets of parentheses: the first for input parameters the second for output (and occasionally input -- via unification) columns.

```delightql
department_employee_count(E(*), D(*))(department, employee_count) :-
  E(*), D(*.(DepartmentId))
    |> %(D.department ~> count:(*) as employee_count)
```

In the above example, the parameters `E(*)` and `D(*)` are *glob parameter
functors* and denote that two tables (or lower-order relations) are expected as
inputs.  The `(*)` in the parameter functor name signals to the compiler that
the body will reference these tables' columns by name.

The term **higher-order rules** is the standard usage throughout this reference, but an equally valid term is **input-moded rules**.

```delightql
foo(input1,input2(*),input3(t,v))(output_column1,output_column2)
  :-  <body>
```

The first set of parentheses, the ones closer to the name of the rule, should
not be understood as a suite of dimensions that may be tables -- although this
is true -- but as parameters that **must** be instantiated and passed in.  In
prolog, this sort of declaration is called a mode and is used to indicate which
dimension needs to be instantiated vs which ones may be output only.  In
delightql input-moded rules require the parameters of the first parentheses to
be input, and *may* allow the columns of the second set of parentheses to be
_input_ -- though this is called unification and or grounding.

