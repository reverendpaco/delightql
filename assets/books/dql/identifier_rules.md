# Case insensitivity {.dqlh}

Delightql is case-insensitive. [In contrast to Prolog, where capitalization
distinguishes variables from atoms.]{.sidenote} The following all refer to the
same identifier:

 - `employeeid`
 - `EmployeeId`
 - `EMPLOYEEID`


## Stropping {.dqlh}

When a name collides with a keyword or contains illegal characters (spaces, for instance), delimit it with backticks: `` `Employee Id` ``.
