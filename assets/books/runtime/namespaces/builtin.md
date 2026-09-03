
# Builtin Namespaces {.dqlh}

Delightql guarantees the following namespaces:

 - `main` -- where data is mounted when a database is provided at start of process
 - `user` -- where consult rules for inline ddl are written
 - `std` -- no entities, just a container for other namespaces
 - `std::prelude` -- auto-enlisted
 - `std::string` -- reserved
 - `std::constants` --reserved
 - `std::documentation` -- where the error and danger uris will go
 - `std::predicates` -- for `like` and `between`

> While introspectable system tables may be seen
> and used ( located at all namespaces with a `sys` prefix)
> they **are not to be depended upon**.

This is a hard choice to allow the slow adoption of
guranteed introspection and metadata predicates into `std`
while their lower-level and version-based siblings at
`sys` may be churned as dictated by functionality and refactoring.

Delightql is already exceptionally well introspectable,
but has not yet decided its public facing contract.

**To repeat:  all namespaces and entities prefixed by `sys` do not
guarantee stability to a programmer over version changes.**

