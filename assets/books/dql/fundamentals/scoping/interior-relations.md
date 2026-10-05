
# Interior relations and scope {.dqlh}

Interior relations create their own scope within the parentheses of a functor.


```delightql
track( , milliseconds<60000 |> (name))
```

An interior relation is constructed by authoring a relational continuation
immediately after the opening (left) parenthesis of a named predicate and
ending the continuation with the closing (right) parenthesis.

The interior relations' scope is delimited and private, but has access to the
surrounding scope's logic variables.  The final current pending relation
(**CPR**) of the interior relation is published under the name of the functor.


```delightql
track(                   //  ①
  , milliseconds<60000   //  ②
  |> (name)              //  ③
  )
  //④  Track.Name
  // is the only logic variable
```

In the above example, a relational continuation is started at position one, and
proceeds through to position three.   The closing of the interior relation
publishes the last **CPR** of the interior relation as the columns of the relation
named `track`.

The term **interior relation** is used for multiple manifestations:

  - simple subqueries
  - lateral joins
  - scalar subqueries
  - EXISTS semi-joins
