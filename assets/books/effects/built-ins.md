
# Built-in Directives {.dqlh}

The built-in directives fall into five categories, by what effects they produce:

| category | directives | direct |
|---|---|---|
| **session directives** | `consult!`, `consult_tree!`, `reconsult!`, `unconsult!`, `mount!`, `mount_new!`, `mount_tree!`, `unmount!`, `refresh!`, `ground!`, `enlist!`, `delist!`, `alias!`, `expose!`, `doc!` | the session's namespace tree |
| **DDL directives** | `temp_table!`, `table!`, `temp_view!`, `imprint!`, `imprint_replace!` | database objects |
| **DML directives** | `insert!`, `update!`, `delete!` | rows in user tables |
| **execution directives** | `run!`, `run_namespace!` | starting runs |
| **utility directives** | `exit!`, `returning!`, `returning_other!`, `stdout!` | the run itself: stop, return, sequence, print |
