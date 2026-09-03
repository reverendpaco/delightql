
# Join {.dqlh}

Joins extend a relation's schema by combining columns from multiple sources.
Every join is a filtered cross product -- the join condition determines which
pairings survive.

Delightql evaluates joins left to right. Each table must be in scope before its
columns can be referenced.
