
# Definition Syntax {.dqlh}

A higher-order rule has two sets of parentheses: the first for input parameters the second for output (and occasionally input -- via unification) columns.

```{.delightql .am}
genre_track_count(T(*), G(*))(genre, track_count) :-
  T(*), G(*.(genre_id))
    |> %(G.name as genre ~> count:(*) as track_count)
```

In the above example, the parameters `T(*)` and `G(*)` are *glob parameter
functors* and denote that two tables (or lower-order relations) are expected as
inputs.  The `(*)` in the parameter functor name signals to the compiler that
the body will reference these tables' columns by name.

The term **higher-order rules** is the standard usage throughout this reference, but an equally valid term is **input-moded rules**.

```{.delightql .am}
long_tracks(n, T(*), G(genre_id, genre))(genre, track_name) :-
  T(*), G(*.(genre_id)), milliseconds > $.n
    |> (genre, name as track_name)
```

The first set of parentheses, the ones closer to the name of the rule, should
not be understood as a suite of dimensions that may be tables -- although this
is true -- but as parameters that **must** be instantiated and passed in.  In
prolog, this sort of declaration is called a mode and is used to indicate which
dimension needs to be instantiated vs which ones may be output only.  In
delightql input-moded rules require the parameters of the first parentheses to
be input, and *may* allow the columns of the second set of parentheses to be
_input_ -- though this is called unification and or grounding.

