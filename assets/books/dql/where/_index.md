# Where {.dqlh}


Selection (σ in Codd's relational algebra, `WHERE` in SQL) filters rows without
changing schema. [SQL's use of "select" for projection is unfortunate -- Codd used
"select" for row filtering. Delightql follows Codd's terminology.]{.sidenote}

Delightql uses the comma (conjunction) to attach predicates to relations. This is the
same syntax as joins and comes directly from Prolog.
