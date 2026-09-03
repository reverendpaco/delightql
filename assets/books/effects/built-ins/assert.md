
# The assert! directive {.dqlh}

The assert! directive takes three arguments:

 - a HO-judgment rule
 - a human readable label
 - the input relation

and runs the input relation through the judgment rule.  If the judgement returns any rows then the assertion has passed. If the judgement fails to return rows, then assert! aborts the script or one REPL invocation and prints the human-readable label.


> Example Passing
>
>
> ```delightql
> _(a@10;22;42)
>   !> assert!(
>        same_bag(
>        _(a@10;22;42)),
>        "Anon tables should match")(*)
>   |> ( a*2 as post_assert)
> ```
>
> | post_assert |
> | ----------- |
> | 20
> | 44
> | 84
>

> Example Failing
>
> ```delightql
> _(a@10;22;42)
>   !> assert!(
>        same_bag(
>        _(a@ 99999999 )),
>        "Anon tables should match")(*)
>   |> ( a*2 as post_assert)
> ```
>
> Error: [delightql-error://runtime/assertion] : Anon tables should match

## Judgment Rules {.dqlh}

A judgment rule is any higher-order rule that takes
a table parameter as its last parameter. Structurally, this is denoted
in higher-order parameter syntax as `P(... T(*))(*)`.

```delightql
at_least(n,T(*))(*) :-
    T(*) ~> count:(*) as count,
    count >= n

is_greater(n, T())(*) :-
    T(*) ~> count:(*) as count,
    count > n

not_empty(T(*))(*) :-
    T(*) ~> count:(*) as count,
    count > 0

greater_than(n, T(*))(*) :-
    T(*) ~> count:(*) as count,
    count > n

count_between(lo, hi, T(*))(*) :-
    T(*) ~> count:(*) as count,
    count >= lo,
    count <= hi
```

A judgment rule **should** encode some simple logic about
when a condition is met or not and return zero or one rows:
zero to cause the system to abort, and one to allow
the chain to continue.


## Denotational Assert! Definition {.dqlh}

The assert rule is assumed to obey the following denotation:

```dql
assert!(P(... T(*))(*), label, I(*))(*) :-
    I(*) |> P(*) : witness
    witness(*) + : judgment
    judgment(*), met=1 |> returning_other!(I(*))(*) : pass!
    judgment(*), met!=1, abort!(label) : fail!

    fail!(*); pass!(*)
```

Roughly this shows three higher-order parameters:

  - the judgment rule ( `P(...T(*))(*)`) that structurally
still requires one last table-value parameter
  - the `label` to be printed on failure and
  - the input relation `I(*)`

The invocation `I(*) |> P(*)` is the heart of the asserts.
