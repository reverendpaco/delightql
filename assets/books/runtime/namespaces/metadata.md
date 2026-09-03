
# Namespace metadata {.dqlh}

## README functor {.dqlh}

Each `std` namespace (any namespace with the prefix `std` -- inclusive of `std`)
is guaranteed to have a `README(*)` entity that returns a one column table
with the column name `readme`.


## Namespace catalog functor {.dqlh}

Every namespace may be called directly:

  `namespace::subnamespace::(*)`

