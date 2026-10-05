# Left-to-Right Evaluation {.dqlh}

Delightql uses a left-to-right evaluation strategy
for determining both what variables are in scope and
the state of the current pending relation (**CPR**).


Within a delightql relational expression, parentheses are **not** used at the relational level to force scope or evaluation -- though they are permitted for grouping and binding domain expressions.

Delightql does have something like parentheses,
but these are called interior relations and are accurately described as named contextual scopes.

## Scope Introduction {.dqlh}

Logic variables are brought into scope by ground relational expressions (**GRELEX**s) either directly or via joining or unioning. A GRELEX is inclusive of anonymous tables and literal references.

```delightql
employee(*)
```

The `employee(*)` GRELEX literal reference introduces its logic variables (LVars) into scope.

The current pending relation (CPR) of each new continuation may grow or shrink or stay the same based on the category of relational operator that is applied in the continuation.


```delightql
employee(*)  //  ①
  ,_(a@3;39) //  ②
```

The `employee(*)` GRELEX introduces
logic variables into scope, followed by the `,_(a@3;39)` JOIN continuation which introduces even more logic variables (just `a`) into scope.


```delightql
artist(*)
  ,album(*.(artist_id))
  |> ( name, title) //  ③
```

The `|> ( name, title)` PROJECTION continuation removes logic variables and establishes a new scope barrier.

## Scope Barrier {.dqlh}

A **scope barrier** replaces the local relational interface: following
continuations cannot recover its old logic variables unless republished.
A pipe inside an interior relation does not also remove bindings from an
enclosing scope. Those remain accessible under the ordinary naming rules,
without automatically becoming columns of the interior's output.

```delightql
album(*) as a
   //①  CPR = [ a.album_id, a.Title, a.artist_id ]
   , artist_id = 22
   //②  CPR = [ a.album_id, a.Title, a.artist_id ]
   |> ( title )
   //③  CPR = [ Title ]
   ,title="Coda"
   //④  no access to a.*.  Only Title
```

After the third continuation which is a **scope barrier**, the fourth continuation
does not have access to the logic variables `a.album_id`, `a.title`, or `a.artist_id`.

Scope barriers are most often post-pipe projection operators, but also include
**metaize** `^` and **witness** `+`.

```delightql
employee(*) ^ // removes Employee's logical variables from scope
```


## Non-commutativity of usage versus introduction {.dqlh}

Because delightql uses a left-to-right evaluation scheme,
a logic variable cannot be used unless it has been brought into scope
to the left.  This is in contrast to other "more declarative"
languages where the usage and the introduction may be swapped.

```{.delightql .bad}
milliseconds<20000, track(*)
```

The above is incorrect as the logic variable `milliseconds` has
not yet been brought into scope via a left-to-right evaluation
strategy.
