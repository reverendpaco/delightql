# Union All Corresponding (`;`{.delightql .sigil}) {.dqlh}

Aligns by name, NULL-padding missing columns. Output schema: first relation's
columns, then non-overlapping columns from the second.[This is closer in definition to OUTER
UNION. The few SQLs that implement UNION ALL CORRESPONDING do so by
outputting the intersection of the two column sets, instead of the union of the
two column sets that delightql favors.]{.sidenote}

```delightql
 _( a,b,c
    -------
    1,2,3;
    4,5,6)
    ;
 _( d,   a,b
   -------
   "foo",10,20;
   "bar",40,50)
```

a | b | c | d
--|---|---|--
1 | 2 | 3 | NULL
4 | 5 | 6 | NULL
10 | 20 | NULL | foo
40 | 50 | NULL | bar

Union All Corresponding is a *ragged* union.
