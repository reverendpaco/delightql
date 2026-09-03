# Higher-order Parameter Chart {.dqlh}

:::{.widen}
| Definition                   | Functor call site                     | `&`?               |    |
|------------------------------|---------------------------------------|--------------------|----|
| `f(T(*), V(*))`              | `f(users(*), orders(*))(*)`           | no                 |    |
| `f(T(*), V(*))`              | `users(*)                             | > f(orders(*))(*)` | no |
| `f(T(*), n)`                 | `f(users(*), 10)(*)`                  | no                 |    |
| `f(T(*), V(*), n)`           | `f(users(*), orders(*), 10)(*)`       | no                 |    |
|                              |                                       |                    |    |
| `f(T(x, y))`                 | `f(data(col1, col2))(*)`              | no                 |    |
| `f(T(x, y))`                 | `f(_(1, 2; 10, 20))(*)`               | no                 |    |
| `f(T(x, y))`                 | `_(1, 2; 10, 20)                      | > f(*)`            | no |
|                              |                                       |                    |    |
| `f(T(*), V(x, y))`           | `f(users(*), _(1, 2))(*)`             | no                 |    |
| `f(T(*), V(x, y))`           | `f(users(*), data(col1, col2))(*)`    | no                 |    |
| `f(T(*), V(x, y))`           | `users(*)                             | > f(_(1, 2))(*)`   | no |
|                              |                                       |                    |    |
| `f(T(*), V(x, y), n)`        | `f(users(*), _(1, 2), 10)(*)`         | no                 |    |
| `f(T(*), V(x, y), n)`        | `f(users(*), _(1, 2; 10, 20), 10)(*)` | no                 |    |
|                              |                                       |                    |    |
| `f(::ns, n, V(x, y))`        | `f(data::prod, "t", _(1, 2))(*)`      | no                 |    |
|                              |                                       |                    |    |
| *Scalar lifting (shorthand)* |                                       |                    |    |
|                              |                                       |                    |    |
| `f(T(x, y))` *(single)*      | `f(1, 2)(*)`                          | no                 |    |
| `f(T(x, y))`                 | `f(1, 2; 10, 20)(*)`                  | no                 |    |
|                              |                                       |                    |    |
| `f(T(*), V(x, y))`           | `f(users(*) & 1, 2)(*)`               | yes                |    |
| `f(T(*), V(x, y))`           | `f(users(*) & 1, 2; 10, 20)(*)`       | yes                |    |
|                              |                                       |                    |    |
| `f(T(x, y), n)`              | `f("a", "b" & 10)(*)`                 | yes                |    |
| `f(T(x, y), V(a, b))`        | `f(1, 2 & 3, 4)(*)`                   | yes                |    |
|                              |                                       |                    |    |
| `f(::ns, n, V(x, y))`        | `f(data::prod & "t" & 1, 2)(*)`       | yes                |    |
::::

The table is divided into two regions.  In the top region, every table
argument uses functor syntax -- `&` is never needed.  In the bottom
region (scalar lifting), bare scalars fill argumentative functors and
`&` marks the boundaries.

Functor syntax is always available and always unambiguous. Scalar
lifting is an optional shorthand for inline data -- use `&` when you
use it, or wrap in `_()` to avoid it.
