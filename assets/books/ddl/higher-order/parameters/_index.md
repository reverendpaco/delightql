
# Parameters to Higher-order rules {.dqlh}

Higher-order parameters come in five flavors, distinguished by **syntax** in
the definition head:

| Form             | Kind                         | Name                            |
|------------------|------------------------------|---------------------------------|
| `T(*)`           | table, nominally accessed    | glob parameter functor          |
| `T(a, b)`        | table, positionally accessed | argumentative parameter functor |
| `n`              | scalar value                 | scalar parameter                |
| `f:()`           | function value               | function parameter              |
| `P(... T(*))(*)` | higher-order residual rule   | rule-valued parameter           |

The syntax alone tells the language what each parameter accepts.
Capitalization is conventional -- programmers *may* uppercase table
parameters and lowercase scalars for readability, but the language
does not require it.
