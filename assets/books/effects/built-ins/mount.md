
# Mount {.dqlh}

Mounting is the process of importing knowledge of tables and views into a
delightql namespace.  No table, view, function or higher-order table can be
accessed without it having been mounted into a namespace.

During the mounting process, delightql will scan the target connection
for a list of all entities at that location and store them into internal
delightql system tables.  This effectively creates a delightql-side information
schema that becomes the system of record


## mount! {.dqlh}

```delightql
mount!("prod.db", "data::prod")(*)
  ->
```

success | operation | path | namespace
--------|-----------|------|----------
1 | mount! | prod.db | data::prod

The directive `mount!` is a higher-order directive. It takes a pair of
higher-order parameters both of which are strings:

 - the target specific connection string
 - the namespace into which this data will become visibile

The namespace must not exist prior to the mount call.

## unmount! {.dqlh}

```delightql
unmount!("data::prod")(*)
```

success | operation | namespace
--------|-----------|------
1 | unmount! | data::prod

## refresh! {.dqlh}

The `refresh!` directive asks delightql to re-connect to the
connection associated with the data namespace and to
discover new or changed entities.  Use this when other
programs may modify the database entities underneath you.

```delightql
refresh!("data::prod")(*)
```

success | operation | namespace
--------|-----------|------
1 | refresh! | data::prod

## mount_new! {.dqlh}

Use `mount_new!` when the connection refers to a non-existent
database. Delightql will create an empty namespace
connected to the target's own empty namespace.

```delightql
mount_new!("test.db", "data::test")(*)
```

success | operation | path | namespace
--------|-----------|------|----------
1 | mount_new! | test.db | data::test

## mount_tree! {.dqlh}

Use `mount_tree!` when the connection has an internal hierarchy
that can be turned into sub-namespaces on the delightql side.
