# Language Fundamentals {.dqlh}

Understanding delightql means internalizing several simple but fundamental choices
that are both semantic and syntactic in nature (and often times both).

 - **Logic variables**: delightql borrows Prolog's unification syntax and semantics, though with a subtle definition of a logic variable as being *qualified*
 - **Null semantics**: delightql opts to provide null-safe equality in contrast to SQL's `=` operator
 - **SQL semantics**: delightql defines its primary semantics via a direct mapping to SQL
 - **Evaluation**: delightql evaluates left-to-right in a manner remeniscent of concatenative languages and eschewing parenthesization
 - **Scoping**: delightql scopes left-to-right with well-known scope barriers that delimit what sections of the code may access which logic variables
 - **Column access**: delightql makes strong choices about how a programmer may access columns by always providing ordinal access, and forbidding named access to ambiguous or generated columns
 - **Moding rules and currying with higher-order rules**:  delightql, like Prolog, has a mechanism to mark syntax as moded for input vs output
