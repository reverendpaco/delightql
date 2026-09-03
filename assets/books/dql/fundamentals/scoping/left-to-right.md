# Left to Right Evaluation {.dqlh}

Delightql uses a left-to-right evaluation strategy
for determining both what variables are in scope and
the state of the current pending relation (**CPR**).


Within a delightql relational expression, parentheses are **not** used at the relational level to force scope or evaluation -- though they are permitted for grouping and binding domain expressions.

Delightql does have something like parentheses,
but these are called interior relations and are accurately described as named contextual scopes.

## Scope Introduction {.dqlh}

Logic variables are brought into scope by ground relational expressions (**GRELEX**s) either directly or via joining or unioning. A GRELEX is inclusive of anonymous tables and literal references.

```delightql
users(*)
```

The `users(*)` GRELEX literal reference introduces its logic variables (LVars) into scope.

The current pending relation (CPR) of each new continuation may grow or shrink or stay the same based on the category of relational operator that is applied in the continuation.


```delightql
users(*)     //  ①
  ,_(a@3;39) //  ②
```

The `users(*)` GRELEX introduces
logic variables into scope, followed by the `,_(a@3;39)` JOIN continuation which introduces even more logic variables (just `a`) into scope.


```delightql
users(*)
  ,orders(*)
  |> ( last_name, order_id) //  ③
```

The `|> ( last_name, order_id)` PROJECTION continuation removes logic variables and establishes a new scope barrier.

## Scope Barrier {.dqlh}

A **scope barrier** is any continuation which
prevents following continuations from accessing logic variables to the left
of the scope barrier.

```delightql
users(*) as u
   //①  CPR = [ u.last_name, u.first_name, u.age]
   , age<50
   //②  CPR = [ u.last_name, u.first_name, u.age]
   |> ( last_name )
   //③  CPR = [ last_name]
   ,last_name="Smith"
   //④  no access to u.*.  Only last_name
```

After the third continuation which is a **scope barrier**, the fourth continuation
does not have access to the logic variables `u.last_name`, `u.first_name`, or `u.age`.

Scope barriers are most often post-pipe projection operators, but also include
**metaize** `^` and **witness** `+`.

```delightql
users(*) ^ // removes the users' logical variables from scope
```


## Non-commutativity of usage versus introduction {.dqlh}

Because delightql uses a left-to-right evaluation scheme,
a logic variable cannot be used unless it has been brought into scope
to the left.  This is in contrast to other "more declarative"
languages where the usage and the introduction may be swapped.

```delightql
age<20, users(*)
```

The above is incorrect as the logic variable `age` has
not yet been brought into scope via a left-to-right evaluation
strategy.
