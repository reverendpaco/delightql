# Variable and Column Scoping {.dqlh}

Within a valid delightql expression the scope of logic variables (columns) is determined by a combination of a left-to-right evaluation strategy and per-operator rules.

The concepts of **relational continuations** and **current pending relation** are interrelated with this left-to-right evaluation strategy.  Further scoping rules are given to interior relations which provide
