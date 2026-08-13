# Formatting {.dqlh}


 All code examples will be inset within colored
 text boxes,  annotated with the primary language. These annotations are located
 atop the upper right corner of the code box.

   * Boxes annotated with `Sql` are explanatory transpilations
   * Boxes annotated with `delightql` are delightql examples of query
     expressions that are available during *query mode*
   * Boxes annotated with `delightql AM` are delightql examples of *assertion
     mode* constructs, for defining rules, tables, updates, etc. (DDL and DML).


The below example shows a simple query in *query mode*:

 ~~~{.delightql  .numberLines  }
 employee(*)
 ~~~

 and the next shows *assertion mode*:


```{.delightql .numberLines .am}
 /* Sigma Rule, column is moded to require instantiation */
 empty(column) :- {}=column
 empty(column) :- trim:(column)=""
 empty(column) :- +no_data(column)
```


 Inline text follows the following typography convention:

 - Sql keyword syntax is italicized in fixed-width font code blocks, such
   as *`update`*, or *`select`* or *`drop`*.
 - Identifiers (regardless of language) are styled as monospace code
   (e.g. `employee`, or `last_name`) and are not italicized.
 - Delightql sigils are bounded by double parentheses, (⸨ ⸩). This is done to
   prevent confusion by separating the punctuation belonging to the language
   from the punctuation of the reference. Examples: `,`{.sigil .delightql}, or
   `|>`{.sigil .delightql}. To repeat: only the syntax within the double
   parentheses (⸨ ⸩) is valid delightql syntax.
 - Delightql sigils are often accompanied by their sigil name in a
   capitalized bold font.  These names provide easier search values within
   the reference. Examples:  **R-PIPE** for `|>`{.delightql .sigil}, **GROUP-MODULO** for
   `%(◌)`{.delightql .sigil}.  Both the sigils and the sigil are found
   in the [DqlVoc] section of this reference.
 - Certain phrases will be italicized, indicating that there is a formal
   definition for this phrase and found in a glossary at the end of the
   reference. Examples:

      - *stacked notation*
      - *sigma clauses*
      - *column ordinality*
      - *current piped relation*
      - etc.
